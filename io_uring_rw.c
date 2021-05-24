#include <linux/fs.h>
#include <linux/slab.h>
#include <linux/aio.h>
#include <linux/bitmap.h>
#include <linux/mm.h>
#include <linux/uio.h>

#include "nova.h"
#include "stats.h"
#include "inode.h"

/*
    for debug:
    io_uring_call : 关键字用于观察调用链
*/
const char * io_uring_call = "io_uring_call";

void io_uring_async_work(struct work_struct *p_work){
    
    nova_info("%s : %s",io_uring_call,__func__);

    struct async_work_struct *a_work = container_of(p_work, struct async_work_struct, awork);
    struct file *filp = a_work->a_iocb->ki_filp;
    struct iovec *iv = &a_work->my_iov;
    ssize_t ret = -EINVAL;


    if (iov_iter_rw(&a_work->iter) == READ){
        ret = nova_dax_file_read(filp,iv->iov_base,iv->iov_len,a_work->ki_pos);
    }

    if (iov_iter_rw(&a_work->iter) == WRITE)
        ret = nova_dax_file_write(filp,iv->iov_base,iv->iov_len,a_work->ki_pos);

quit:
    struct inode *inode = filp->f_mapping->host;
    unsigned long first_blk, nr, wtbit, wkbit;
    struct nova_inode_info *ino_info;
    struct async_work_struct *tmp_contn;
    ino_info = container_of(inode, struct nova_inode_info, vfs_inode);
    struct super_block *sb = ino_info->vfs_inode.i_sb;
    first_blk = a_work->first_blk;
    nr = a_work->blknr;
    //queue work from conflict queue
    if (!list_empty(&a_work->aio_conflicq))
    {
        tmp_contn = container_of(a_work->aio_conflicq.next, struct async_work_struct, aio_conflicq);
        tmp_contn->first_blk = a_work->first_blk;
        tmp_contn->blknr = a_work->blknr;

        spin_lock(&ino_info->aio.wk_bitmap_lock);
        /*use wk_bitmap_lock to achieve atomic*/
        if (--(*(a_work->nr_segs)) == 0)
        {
            kfree(a_work->nr_segs);

            kiocb_done(a_work->a_iocb,ret);
            //a_work->a_iocb->ki_complete(a_work->a_iocb, ret, 0);
        }

        spin_unlock(&ino_info->aio.wk_bitmap_lock);

        list_del(&a_work->aio_conflicq);
        INIT_WORK(&(tmp_contn->awork), nova_async_work);
        tmp_contn->isQue = queue_work(sb->s_dio_done_wq, &(tmp_contn->awork));
    }
    else
    {
        //conflict queue empty, clear workbitmap
        wkbit = first_blk;

        spin_lock(&ino_info->aio.wk_bitmap_lock);
        /*use wk_bitmap_lock to achieve atomic*/
        if (--(*(a_work->nr_segs)) == 0)
        {
            kfree(a_work->nr_segs);
            kiocb_done(a_work->a_iocb,ret);
            //->a_iocb->ki_complete(a_work->a_iocb, ret, 0);
        }
        // nova_info("%s : clear work_bitmap first_blk : %lu,nr :%lu\n",__func__,first_blk,nr);
        for_each_set_bit_from(wkbit, ino_info->aio.work_bitmap, first_blk + nr)
            clear_bit(wkbit, ino_info->aio.work_bitmap);
        if (ino_info->aio.i_waitque.next != &ino_info->aio.i_waitque)
            queue_wait_work(ino_info);
        spin_unlock(&ino_info->aio.wk_bitmap_lock);

        //check waitqueue
        //if (spin_trylock(&inode->i_lock))
        // {
        //     queue_wait_work(ino_info);
        //     spin_unlock(&inode->i_lock);
        // }
    }

    kfree(a_work);
    a_work = NULL;
}



/*
    io_uring 读写流程： 应用程序 设置SQEs，调用io_uring_enter 陷入内核。对SQES进行处理.对每个读写请求调用
    nova_dax_write_iter->nova_io_uring_rw.这个函数将请求进行处理，放入队列中.从而实现异步

*/
ssize_t nova_io_uring_rw(struct kiocb *iocb, struct iov_iter *iter)
{

    nova_info("%s : %s",io_uring_call,__func__);

    struct file *filp = iocb->ki_filp;
    struct inode *inode = filp->f_mapping->host;
    struct iovec *iv = iter->iov;
    struct nova_inode_info *ino_info;
    struct super_block *sb = inode->i_sb;
    struct async_work_struct *io_work;

    loff_t end = iocb->ki_pos; /* notice ,we should  not use iter->iov_offset,because iter->iov_offset is always  zero*/
    ssize_t ret = -EINVAL;
    unsigned long seg, nr_segs = iter->nr_segs;
    unsigned long size, i_blocks; /* support maximum file size is 4G */

    /*if file need to grow,we should add multiple write request(nr_segs>1)
          to same conflict queue;because we need to ensure atomicity;
    */
    bool need_grow = false;

    ino_info = container_of(inode, struct nova_inode_info, vfs_inode);

    for (seg = 0; seg < nr_segs; seg++)
    {
        end += iv->iov_len;
        iv++;
    }
    iv = iter->iov;
    // nova_info("iocb->pos: %ld, iter->iov_offset: %ld, end : %lu\n",iocb->ki_pos,iter->iov_offset,end);

    if (!is_sync_kiocb(iocb))
    {

        if (!sb->s_dio_done_wq)
            ret = sb_init_wq(sb);
        spin_lock(&inode->i_lock);

        if (iocb->ki_pos > inode->i_size)
        {
            nova_info("%s: iocb->ki_pos is too big !\n", __func__);
            spin_unlock(&inode->i_lock);
            return ret;
        }

        size = end;
        i_blocks = inode->i_blocks; /*inode->i_blocks is  4kb*/
        if (size > inode->i_size)
        {
            if ((iov_iter_rw(iter) == WRITE))
            {
                i_blocks = DIV_ROUND_UP(size, PAGE_SIZE);
                need_grow = true;
            }
        }
        else
        {
            size = inode->i_size;
        }

        //async bitmap init
        if (!ino_info->aio.wait_bitmap)
        {
            ino_info->aio.wait_bitmap = kzalloc(BITS_TO_LONGS(i_blocks) * sizeof(long), GFP_KERNEL);
            ino_info->aio.work_bitmap = kzalloc(BITS_TO_LONGS(i_blocks) * sizeof(long), GFP_KERNEL);
            ino_info->aio.bitmap_size = BITS_TO_LONGS(i_blocks) * sizeof(long);
            // nova_info("%s :alloc bitmap ino_info->aio.bitmap_size=%lu\n",__func__, ino_info->aio.bitmap_size);
        }
        else
        {
            //spin_lock(&ino_info->aio.wk_bitmap_lock);
            if (ino_info->aio.bitmap_size * BITS_PER_BYTE < i_blocks)
            {
                unsigned long *tmp = kzalloc(BITS_TO_LONGS(i_blocks) * sizeof(long), GFP_KERNEL);
                memcpy((void *)ino_info->aio.wait_bitmap, (void *)tmp, ino_info->aio.bitmap_size);
                kfree(ino_info->aio.wait_bitmap);
                ino_info->aio.wait_bitmap = tmp;
                tmp = kzalloc(BITS_TO_LONGS(i_blocks) * sizeof(long), GFP_KERNEL);
                memcpy((void *)ino_info->aio.work_bitmap, (void *)tmp, ino_info->aio.bitmap_size);
                kfree(ino_info->aio.work_bitmap);
                ino_info->aio.work_bitmap = tmp;
                ino_info->aio.bitmap_size = BITS_TO_LONGS(i_blocks) * sizeof(long);
                // nova_info("%s :realloc bitmap ino_info->aio.bitmap_size=%d\n",__func__, ino_info->aio.bitmap_size);
            }
            //spin_unlock(&ino_info->aio.wk_bitmap_lock);
        }

        if (!ino_info->aio.wait_bitmap || !ino_info->aio.work_bitmap)
        {
            kfree(ino_info->aio.wait_bitmap);
            kfree(ino_info->aio.work_bitmap);
            ino_info->aio.bitmap_size = 0;
            // nova_info("%s :ino_info->aio.wait_bitmap if free and exit,notice we don't unlock_spinlock!\n",__func__);
            return -ENOMEM;
        }

        /* async_work_struct(I/O node in file waitqueue) */

        unsigned long first_blk, nr, off, wtbit, wkbit;
        unsigned long *temp_seg = (unsigned long *)kzalloc(sizeof(unsigned long), GFP_KERNEL);
        *temp_seg = nr_segs;
        struct list_head *async_pos;
        struct async_work_struct *contn = NULL;
        loff_t pos;

        seg = 0;
        end = iocb->ki_pos;
        size = inode->i_size;

        if (!need_grow || iov_iter_rw(iter) == READ)
            spin_unlock(&inode->i_lock);
        need_grow = false;

        while (seg < nr_segs)
        {
            io_work = (struct async_work_struct *)kzalloc(sizeof(struct async_work_struct), GFP_KERNEL);
            pos = end;
            end += iv->iov_len;
            if (end > size)
            {
                if ((iov_iter_rw(iter) == WRITE))
                    need_grow = true;
                else
                {
                    end = size;
                    *temp_seg = seg + 1;
                    seg = nr_segs;
                }
            }

            memcpy(&io_work->iter, iter, sizeof(struct iov_iter));
            io_work->id = id++;
            io_work->my_iov.iov_base = iv->iov_base;
            io_work->my_iov.iov_len = end - pos;
            io_work->ki_pos = pos;
            io_work->a_iocb = iocb;
            io_work->nr_segs = temp_seg;
            io_work->tsk = current;
            io_work->isQue = 0;
            INIT_LIST_HEAD(&io_work->aio_waitq);
            INIT_LIST_HEAD(&io_work->aio_conflicq);

            first_blk = pos >> PAGE_SHIFT; // notice: if end =0;first_blk == 0?
            off = pos & (PAGE_SIZE - 1);
            nr = DIV_ROUND_UP(iv->iov_len + off, PAGE_SIZE);
            io_work->first_blk = first_blk;
            io_work->blknr = nr;

            //nova_info("%s: io_work : %d,io_work->my_iov.iov_bashe:%p , io_work->my_iov.iov_len : %lu,io_work->ki_pos:%lu\n",
            //          __func__, io_work->id, io_work->my_iov.iov_base, io_work->my_iov.iov_len, io_work->ki_pos);

            iv++;
            seg++;

            if (need_grow)
            {
                if (contn == NULL)
                {
                    contn = io_work;
                }
                else
                {
                    /*if file need to grow,we should add multiple write request(nr_segs>1)
                      to same conflict queue;because we need to ensure atomicity;
                    */
                    list_add_tail(&io_work->aio_conflicq, &contn->aio_conflicq);
                    // nova_info("%s :conflicq queue: head id: %d add id: %d ",__func__,contn->id,io_work->id);
                }

                if (seg == nr_segs)
                {
                    /*for file grow , we should update inode->i_size first*/
                    inode->i_size = end;
                    spin_unlock(&inode->i_lock); /*we hold this lock for update inode->i_size*/

                    first_blk = contn->first_blk;
                    nr = DIV_ROUND_UP(end - contn->ki_pos, PAGE_SIZE);
                    io_work = contn;
                    io_work->blknr = nr;

                    /* for test*/
                    //nova_info("%s : conficq queue : ",__func__);
                    // struct async_work_struct *head = contn;
                    // do{
                    //     nova_info("%d -->",head->id);
                    //     head = container_of(head->aio_conflicq.next, struct async_work_struct,aio_conflicq);
                    // }while(head != contn);
                    // nova_info("tail \n");
                    /* for test*/
                }
                else
                    continue;
            }

            spin_lock(&ino_info->aio.wk_bitmap_lock);
            wtbit = first_blk;
            wtbit = find_next_bit(ino_info->aio.wait_bitmap, first_blk + nr, wtbit);
            wkbit = first_blk;
            wkbit = find_next_bit(ino_info->aio.work_bitmap, first_blk + nr, wkbit);
            // nova_info("%s : io_work->id:%d,wtbit:%lu,wkbit : %lu\n",__func__,io_work->id,wtbit,wkbit);

            if (wtbit < first_blk + nr || wkbit < first_blk + nr)
            {
                if (wtbit >= first_blk + nr)
                { // 01
                    //nova_info("id :%d ,first_blk :%lu,nr : %lu\n",io_work->id,first_blk,nr);
                    //nova_info("%s : add io_work: %d to ino_info->waitque \n", __func__, io_work->id);
                    wtbit = first_blk;

                    for_each_clear_bit_from(wtbit, ino_info->aio.wait_bitmap, first_blk + nr)
                        set_bit(wtbit, ino_info->aio.wait_bitmap);
                    async_pos = ino_info->aio.i_waitque.next;
                    while (async_pos != &ino_info->aio.i_waitque)
                    {
                        contn = container_of(async_pos, struct async_work_struct, aio_waitq);
                        if (contn->first_blk > first_blk)
                            break;
                        async_pos = async_pos->next;
                    }
                    list_add_tail(&io_work->aio_waitq, async_pos);
                    spin_unlock(&ino_info->aio.wk_bitmap_lock);
                }
                else
                { //10 11

                    /*for test*/

                    // nova_info("id:%d ,first_blk :%lu,nr : %lu\n",io_work->id,first_blk,nr);
                    //nova_info("%s : add io_work :%d to conficq\n", __func__, io_work->id);
                    //async_pos = ino_info->aio.i_waitque.next;
                    //nova_info("%s : wait queue : ",__func__);
                    //while(async_pos != &ino_info->aio.i_waitque){
                    //    contn = container_of(async_pos, struct async_work_struct, aio_waitq);
                    //nova_info("%d -->",contn->id);
                    //     async_pos = async_pos->next;
                    //}
                    /*for test*/

                    unsigned long f_blk, t_nr, t_bit;
                    async_pos = ino_info->aio.i_waitque.next;
                    while (async_pos != &ino_info->aio.i_waitque)
                    {
                        contn = container_of(async_pos, struct async_work_struct, aio_waitq);
                        f_blk = contn->first_blk;
                        if (f_blk + contn->blknr >= first_blk && f_blk <= first_blk + nr)
                        {
                            if (async_pos->next != &ino_info->aio.i_waitque)
                            {
                                struct async_work_struct *tmp_contn;
                                tmp_contn = container_of(async_pos->next, struct async_work_struct, aio_waitq);
                                if (tmp_contn->first_blk < first_blk + nr)
                                {
                                    async_pos = async_pos->next;
                                    list_del(&contn->aio_waitq);
                                    /*splice two list*/

                                    list_merge(&contn->aio_conflicq, tmp_contn->aio_conflicq.prev, &tmp_contn->aio_conflicq);
                                    tmp_contn->blknr += (tmp_contn->first_blk - contn->first_blk);
                                    tmp_contn->first_blk = contn->first_blk;
                                }
                                else
                                    break;
                            }
                            else
                                break;
                        }
                        else
                        {
                            if (f_blk > first_blk + nr)
                            {
                                contn = container_of(async_pos->prev, struct async_work_struct, aio_waitq);
                                break;
                            }
                            async_pos = async_pos->next;
                        }
                    }
                    async_pos = &contn->aio_conflicq;
                    list_add_tail(&io_work->aio_conflicq, async_pos);

                    t_bit = first_blk;
                    for_each_clear_bit_from(t_bit, ino_info->aio.wait_bitmap, first_blk + nr)
                        set_bit(t_bit, ino_info->aio.wait_bitmap);

                    if (contn->first_blk > io_work->first_blk)
                        contn->first_blk = io_work->first_blk;

                    /*you can draw to understanding*/
                    f_blk = contn->blknr + contn->first_blk - io_work->first_blk;
                    t_nr = io_work->first_blk + io_work->blknr - contn->first_blk;
                    if (f_blk > t_nr)
                        contn->blknr = f_blk;
                    else
                        contn->blknr = t_nr;
                    spin_unlock(&ino_info->aio.wk_bitmap_lock);
                }
            }
            else
            { //00
                //nova_info("%s : add io_work : %d to work\n", __func__, io_work->id);

                wkbit = first_blk;
                for_each_clear_bit_from(wkbit, ino_info->aio.work_bitmap, first_blk + nr)
                    set_bit(wkbit, ino_info->aio.work_bitmap);

                spin_unlock(&ino_info->aio.wk_bitmap_lock);

                INIT_WORK(&(io_work->awork), io_uring_async_work);
                io_work->isQue = queue_work(sb->s_dio_done_wq, &(io_work->awork));
            }
        }
    } /* async*/

    /*for test ,count running time*/
    //unsigned long time;
    //TEST_END_TIMING(1, time, test_start);
    //nova_info("=========%s,work %d use time: %lu==============\n", __func__, id - 1, time);
    /*for test*/

    return -EIOCBQUEUED;
}
