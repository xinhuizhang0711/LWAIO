/*
 * BRIEF DESCRIPTION
 *
 * File operations for directories.
 *
 * Copyright 2015-2016 Regents of the University of California,
 * UCSD Non-Volatile Systems Lab, Andiry Xu <jix024@cs.ucsd.edu>
 * Copyright 2012-2013 Intel Corporation
 * Copyright 2009-2011 Marco Stornelli <marco.stornelli@gmail.com>
 * Copyright 2003 Sony Corporation
 * Copyright 2003 Matsushita Electric Industrial Co., Ltd.
 * 2003-2004 (c) MontaVista Software, Inc. , Steve Longerbeam
 * This file is licensed under the terms of the GNU General Public
 * License version 2. This program is licensed "as is" without any
 * warranty of any kind, whether express or implied.
 */
#include <linux/fs.h>
#include <linux/slab.h>
#include <linux/aio.h>
#include <linux/bitmap.h>
#include <linux/mm.h>
#include <linux/uio.h>

#include "nova.h"
#include "stats.h"
#include "inode.h"

/*for test,count runing time*/

typedef struct timespec timing_t;

#define INIT_TIMING(X) timing_t X = {0}

#define TEST_START_TIMING(name, _start) \
    getrawmonotonic(&_start)

#define TEST_END_TIMING(name, time, start) \
    INIT_TIMING(_end##name);               \
    getrawmonotonic(&_end##name);          \
    time = (_end##name.tv_sec - start.tv_sec) * 1000000000 + (_end##name.tv_nsec - start.tv_nsec)

/*for test*/

// ssize_t do_nova_cow_file_write_async(struct file *filp, struct page **kaddr, void *buf,
//                                      size_t len, loff_t ppos)
// {
//     return -EIO;
// }

/*
@filp: point to file struct
@kaddr: Virtual address corresponding to buffer
@len: length to write
@ppos : offset of this write
*/
ssize_t do_nova_inplace_file_write_async(struct file *filp, struct page **kaddr, void *buf,
                                         size_t len, loff_t ppos, int id)
{

    struct address_space *mapping = filp->f_mapping;
    struct inode *inode = mapping->host;
    struct nova_inode_info *si = NOVA_I(inode);
    struct nova_inode_info_header *sih = &si->header;
    struct super_block *sb = inode->i_sb;
    struct nova_inode *pi, inode_copy;
    struct nova_file_write_entry *entry;
    struct nova_file_write_entry *entryc, entry_copy;
    struct nova_file_write_entry entry_data;
    struct nova_inode_update update;
    loff_t pos;
    size_t bytes, count, offset, copied, all_copied = 0;
    unsigned long start_blk, total_blocks, num_blocks, ent_blks = 0, blocknr = 0;
    unsigned long remain_len = 0, copied_once = 0, kpage_i = 0, page_offset, new_blocks = 0;
    u64 blk_off, file_size;
    u64 begin_tail = 0;
    u64 epoch_id;
    u32 time = 0;
    int allocate = 0, inplace = 0, step = 0;
    unsigned int data_bits;
    ssize_t ret;
    long status;
    bool hole_fill, update_log = false;
    void *kmem;

    pos = ppos;
    count = len;
    if (filp->f_flags & O_APPEND)
        pos = i_size_read(inode);
    page_offset = pos & (PAGE_SIZE - 1);
    total_blocks = DIV_ROUND_UP(count + page_offset, PAGE_SIZE);

    pi = nova_get_block(sb, sih->pi_addr);
    if (nova_check_inode_integrity(sb, sih->ino, sih->pi_addr, sih->alter_pi_addr, &inode_copy, 0) < 0)
    {
        ret = -EIO;
        goto out;
    }

    /* Does file_remove_privs need a lock ? */
    ret = file_remove_privs(filp);
    if (ret)
        goto out;

    /* maybe we should hold a lock */
    epoch_id = nova_get_epoch_id(sb);

    nova_dbg("%s: id:%d epoch_id %llu, inode %lu, offset %lld, count %lu\n",
             __func__, id, epoch_id, inode->i_ino, pos, count);

    num_blocks = total_blocks;
    while (num_blocks > 0)
    {
        hole_fill = false;
        /* offset of actual file block*/
        offset = pos & (nova_inode_blk_size(sih) - 1);
        start_blk = pos >> sb->s_blocksize_bits;
        ent_blks = nova_check_existing_entry(sb, inode, num_blocks,
                                             start_blk, &entry, &entry_copy,
                                             1, epoch_id, &inplace, 1);
        entryc = (metadata_csum == 0) ? entry : &entry_copy;
        if (entry && inplace)
        {
            blocknr = get_nvmm(sb, sih, entryc, start_blk);
            blk_off = blocknr << PAGE_SHIFT;
            allocate = ent_blks;
            // nova_info("%s :has found entry, blocknum : %d,blocknr: %lu\n",__func__,allocate,blocknr);
            if (data_csum || data_parity)
                nova_set_write_entry_updating(sb, entry, 1);
        }
        else
        {
            /* allocate blocks to fill hole*/
            /* notice:we don't have a lock,because per_cpu have a free_list.but if 
                current cpu has no free block,should we have to hold a lock?
             */
            allocate = nova_new_data_blocks(sb, sih, &blocknr,
                                            start_blk, ent_blks, ALLOC_NO_INIT,
                                            ANY_CPU, ALLOC_FROM_HEAD);
            nova_dbg("%s: alloc %d blocks,size : %lu @ %lu\n",
                     __func__, allocate, allocate * 4096, blocknr);
            if (allocate <= 0)
            {
                nova_dbg("%s alloc blocks failed !% d\n", __func__, allocate);
                ret = allocate;
                goto out;
            }
            hole_fill = true;
            new_blocks += allocate;
            blk_off = nova_get_block_off(sb, blocknr, sih->i_blk_type);
        }
        step++;
        bytes = sb->s_blocksize * allocate - offset;
        if (bytes > count)
            bytes = count;
        kmem = nova_get_block(inode->i_sb, blk_off);

        if (hole_fill &&
            (offset || ((offset + bytes) & (PAGE_SIZE - 1)) != 0))
        {
            nova_info("%s :we should handle_head_tail_blocks!\n", __func__);
            ret = nova_handle_head_tail_blocks(sb, inode,
                                               pos, bytes, kmem);
            if (ret)
                goto out;
        }

        /*now copy from user buf*/

        nova_memunlock_range(sb, kmem + offset, bytes);
        page_offset = buf&(PAGE_SIZE-1);
        remain_len = bytes;

        while (remain_len > 0)
        {
            /*copied_once should not lengther than PAGE_SIZE*/
            copied_once = ((remain_len - 1) & (PAGE_SIZE - 1)) + 1 - page_offset;

            /*If buf is not aligned 4kb,kaddr[0] is aligned 4kb
               such as a write :buf is 2048 len = 2048;
               on count: kaddr[0] = 0 ;count = 2048
               so 0-2048 should be skiped;   buf &(PAGE_SIZE-1) = 2048;
             */

            copied = copied_once - memcpy_to_pmem_nocache(kmem + offset, (char *)kaddr[kpage_i] + page_offset, copied_once);
            all_copied += copied;
            remain_len -= copied;
            offset += copied;
            page_offset = (page_offset + copied) & (PAGE_SIZE - 1);

            if (copied_once == 0)
            {
                nova_dbg("%s : copied_once is zero!, remain_len is %lu\n", __func__, remain_len);
                break;
            }
            else
            {
                if (remain_len & (PAGE_SIZE - 1) == 0)
                    kpage_i++;
            }
        }
        nova_memlock_range(sb, kmem + offset - all_copied, bytes);
        //NOVA_END_TIMING(memcpy_w_nvmm_t, memcpy_time);

        if (pos + all_copied > inode->i_size)
            file_size = cpu_to_le64(pos + all_copied);
        else
            file_size = cpu_to_le64(inode->i_size);

        if (hole_fill)
        {
            /* if this write need to append write_entry(alloc new block),the situation is complex
                1.we do not want other write_entry append in the same place
                2.Even if we solved the first problem,when we has a error during writting ,we need to 
                  clear this incomplete write
                so (append write_entry) should hold a lock all the time 
             */
            if (!update_log)
            {
                //spin_lock(&inode->i_lock);
                update_log = true;
            }

            update.tail = sih->log_tail;
            update.alter_tail = sih->alter_log_tail;

            // fix me ,time always is zero
            nova_init_file_write_entry(sb, sih, &entry_data,
                                       epoch_id, start_blk, allocate,
                                       blocknr, time, file_size);
            ret = nova_append_file_write_entry(sb, pi, inode, &entry_data, &update);

            if (begin_tail == 0)
                begin_tail = update.curr_entry;
            if (ret)
            {
                nova_dbg("%s: append inode entry failed\n", __func__);
                ret = -ENOSPC;
                goto out;
            }
        }
        else
        {
            /*update existing entry*/
            struct nova_log_entry_info entry_info;
            entry_info.type = FILE_WRITE;
            entry_info.epoch_id = epoch_id;
            entry_info.trans_id = sih->trans_id;
            entry_info.time = time;
            entry_info.file_size = file_size;
            entry_info.inplace = 1;
            nova_inplace_update_write_entry(sb, inode, entry,
                                            &entry_info);
        }
        //nova_info("end work: id:%d,Write:%p,%lu\n", id, kmem, all_copied);
        if (all_copied > 0)
        {
            status = all_copied;
            pos += all_copied;
            count -= all_copied;
            num_blocks -= allocate;
        }
        if (unlikely(all_copied != bytes))
        {
            nova_dbg("%s ERROR!: %p, bytes %lu, copied %lu\n",
                     __func__, kmem, bytes, all_copied);
            if (status >= 0)
                status = -EFAULT;
        }
        if (status < 0)
            break;
    }
    data_bits = blk_type_to_shift[sih->i_blk_type];

    if (update_log)
    {
        nova_memunlock_inode(sb, pi);
        nova_update_inode(sb, inode, pi, &update, 1);
        nova_memlock_inode(sb, pi);
        NOVA_STATS_ADD(inplace_new_blocks, 1);

        /* Update file tree ,we don't need lock*/
        ret = nova_reassign_file_tree(sb, sih, begin_tail);
    }
    ret = all_copied;
    NOVA_STATS_ADD(inplace_write_breaks, step);

    sih->i_blocks += (new_blocks << (data_bits - sb->s_blocksize_bits));
    // nova_info("%s :new_blocks %lu,data_bits :%d,sb->s_blocksize_bits:%d,inode->i_blocks:%d\n",__func__,new_blocks,data_bits,sb->s_blocksize_bits,sih->i_blocks);
    inode->i_blocks = sih->i_blocks;
    inode->i_ctime = inode->i_mtime = current_time(inode);

    if (pos > inode->i_size)
    {
        i_size_write(inode, pos);
        sih->i_size = pos;
    }
    sih->trans_id++;
    if (ret)
        goto out;

out:
    /*fix this*/
    if (ret < 0)
        nova_cleanup_incomplete_write(sb, sih, blocknr, allocate,
                                      begin_tail, update.tail);
    if (update_log)
        spin_unlock(&inode->i_lock);
    //NOVA_END_TIMING(inplace_write_t, inplace_write_time);
    NOVA_STATS_ADD(inplace_write_bytes, all_copied);
    return ret;
}

int nova_protect_file_data_pages(struct super_block *sb, struct inode *inode,
                                 loff_t pos, size_t count, struct page **kaddr,
                                 unsigned long blocknr, bool inplace)
{
    struct nova_inode_info *si = NOVA_I(inode);
    struct nova_inode_info_header *sih = &si->header;
    size_t offset, eblk_offset, bytes;
    unsigned long start_blk, end_blk, num_blocks, nvmm, nvmmoff;
    unsigned long blocksize = sb->s_blocksize;
    unsigned int blocksize_bits = sb->s_blocksize_bits;
    u8 *blockbuf, *blockptr;
    struct nova_file_write_entry *entry;
    struct nova_file_write_entry *entryc, entry_copy;
    bool mapped, nvmm_ok;
    int ret = 0;

    size_t remain_len;
    size_t to_copy, kpage_i;
    void *copied;

    INIT_TIMING(protect_file_data_time);
    INIT_TIMING(memcpy_time);

    NOVA_START_TIMING(protect_file_data_t, protect_file_data_time);

    offset = pos & (blocksize - 1);
    num_blocks = ((offset + count - 1) >> blocksize_bits) + 1;
    start_blk = pos >> blocksize_bits;
    end_blk = start_blk + num_blocks - 1;

    NOVA_START_TIMING(protect_memcpy_t, memcpy_time);
    blockbuf = kmalloc(blocksize, GFP_KERNEL);
    if (!blockbuf)
    {
        nova_err(sb, "%s: block bufer allocation error\n", __func__);
        return -ENOMEM;
    }

    bytes = blocksize - offset;
    if (bytes > count)
        bytes = count;

    // copy mapped pages from user to blockbuf page by page
    remain_len = bytes;
    kpage_i = 0;
    copied = NULL;
    do
    {
        to_copy = remain_len < PAGE_SIZE ? remain_len : PAGE_SIZE;
        copied = memcpy(blockbuf + offset + kpage_i * PAGE_SIZE,
                        kaddr[kpage_i], to_copy);

        if (unlikely(!copied))
        {
            NOVA_END_TIMING(protect_memcpy_t, memcpy_time);
            nova_err(sb, "%s: not all data is copied from user! expect to copy %zu bytes, actually copied %zu bytes\n",
                     __func__, bytes, bytes - remain_len);
            ret = -EFAULT;
            goto out;
        }

        remain_len -= to_copy;
        kpage_i++;
    } while (remain_len);

    NOVA_END_TIMING(protect_memcpy_t, memcpy_time);

    entryc = (metadata_csum == 0) ? entry : &entry_copy;

    /*
   * if offset is not zero, find the entry and copy the whole start block to blockbuf
   * if entry doesn't exist, set the start block to zero from start to offset.
   */
    if (offset != 0)
    {
        NOVA_STATS_ADD(protect_head, 1);
        entry = nova_get_write_entry(sb, sih, start_blk);
        if (entry)
        {
            if (!metadata_csum)
                entryc = entry;
            else if (!nova_verify_entry_csum(sb, entry, entryc))
                return -EIO;

            nvmm = get_nvmm(sb, sih, entryc, start_blk); // get data
            nvmmoff = nova_get_block_off(sb, nvmm, sih->i_blk_type);
            blockptr = (u8 *)nova_get_block(sb, nvmmoff); // get pmem address

            mapped = nova_find_pgoff_in_vma(inode, start_blk);
            // data not in dram and not written inplace
            // check the csum
            if (data_csum > 0 && !mapped && !inplace)
            {
                nvmm_ok = nova_verify_data_csum(sb, sih, nvmm, 0, offset);
                if (!nvmm_ok)
                {
                    ret = -EIO;
                    goto out;
                }
            }

            ret = memcpy_mcsafe(blockbuf, blockptr, offset);
            if (ret < 0)
                goto out;
        }
        else
        {
            memset(blockbuf, 0, offset);
        } // if (entry)

    } // if (offset)

    if (num_blocks == 1)
        goto eblk;

    // update the start block's checksum
    kpage_i = 0;
    remain_len = 0;
    copied = NULL;
    do
    {
        if (inplace)
            nova_update_block_csum_parity(sb, sih, blockbuf, blocknr, offset, bytes);
        else
            nova_update_block_csum_parity(sb, sih, blockbuf, blocknr, 0, blocksize);

        blocknr++;
        pos += bytes;
        // buf += bytes;
        count -= bytes;
        offset = pos & (blocksize - 1);
        bytes = count < blocksize ? count : blocksize;

        remain_len = bytes;
        // copy to blockbuf page by page
        do
        {
            to_copy = remain_len < PAGE_SIZE ? remain_len : PAGE_SIZE;
            copied = memcpy(blockbuf + kpage_i * PAGE_SIZE,
                            kaddr[kpage_i], to_copy);
            if (unlikely(!copied))
            {
                nova_err(sb, "%s: not all data is copied from user!  expect to copy %zu bytes, actually copied %zu bytes\n",
                         __func__, bytes, kpage_i * PAGE_SIZE - to_copy);
                ret = -EFAULT;
                goto out;
            }
            remain_len -= to_copy;
            kpage_i++;
        } while (remain_len);

    } while (count > blocksize);

eblk:
    eblk_offset = (pos + count) & (blocksize - 1);

    if (eblk_offset)
    {
        NOVA_STATS_ADD(protect_tail, 1);
        entry = nova_get_write_entry(sb, sih, end_blk);

        if (entry)
        {
            if (metadata_csum)
                entryc = entry;
            else if (!nova_verify_entry_csum(sb, entry, entryc))
                return -EIO;

            nvmm = get_nvmm(sb, sih, entryc, end_blk);
            nvmmoff = nova_get_block_off(sb, nvmm, sih->i_blk_type);
            blockptr = (u8 *)nova_get_block(sb, nvmmoff);

            mapped = nova_find_pgoff_in_vma(inode, end_blk);
            if (data_csum > 0 && !mapped && !inplace)
            {
                nvmm_ok = nova_verify_data_csum(sb, sih, nvmm,
                                                eblk_offset, blocksize - eblk_offset);
                if (!nvmm_ok)
                {
                    ret = -EIO;
                    goto out;
                }
            }

            ret = memcpy_mcsafe(blockbuf + eblk_offset,
                                blockptr + eblk_offset,
                                blocksize - eblk_offset);
            if (ret < 0)
                goto out;
        }
        else
        {
            memset(blockbuf + eblk_offset, 0,
                   blocksize - eblk_offset);
        }
    }

    if (inplace)
        nova_update_block_csum_parity(sb, sih, blockbuf, blocknr,
                                      offset, bytes);
    else
        nova_update_block_csum_parity(sb, sih, blockbuf, blocknr,
                                      0, blocksize);
out:
    if (blockbuf != NULL)
        kfree(blockbuf);

    NOVA_END_TIMING(protect_file_data_t, protect_file_data_time);
    return 0;
}
/*
 * perform a COW write 
 * hold the inode lock before calling !
 * this function has not tested;
 */
static ssize_t do_nova_cow_file_write_async(struct file *filp,
                                            struct page **kaddr, void *buf, size_t len, loff_t ppos, int id)
{
    struct address_space *mapping = filp->f_mapping;
    struct inode *inode = mapping->host;
    struct nova_inode_info *si = NOVA_I(inode);
    struct nova_inode_info_header *sih = &si->header;
    struct super_block *sb = inode->i_sb;
    struct nova_inode *pi, inode_copy;
    struct nova_file_write_entry entry_data;
    struct nova_inode_update update;
    ssize_t written = 0;

    loff_t pos;
    unsigned long start_blk, num_blocks;
    unsigned long total_blocks;
    unsigned long blocknr = 0;
    unsigned int data_bits;
    int allocated = 0;
    void *kmem;
    u64 file_size;
    size_t bytes;
    long status = 0;
    INIT_TIMING(cow_write_time);
    INIT_TIMING(memcpy_time);
    unsigned long step = 0;
    ssize_t ret;
    u64 begin_tail = 0;
    int try_inplace = 0;
    u64 epoch_id;
    u32 time;

    size_t count, page_offset, offset, copied;
    size_t all_copied = 0;
    size_t remain_bytes = 0;
    size_t to_copy = 0;
    unsigned kpage_i = 0;

    if (len == 0)
        return 0;

    NOVA_START_TIMING(do_cow_write_async_t, cow_write_time);

    pos = ppos;

    if (filp->f_flags & O_APPEND)
        pos = i_size_read(inode);

    count = len;

    pi = nova_get_block(sb, sih->pi_addr);
    if (nova_check_inode_integrity(sb, sih->ino, sih->pi_addr,
                                   sih->alter_pi_addr, &inode_copy, 0) < 0)
    {
        ret = -EIO;
        goto out;
    }

    offset = pos & (sb->s_blocksize - 1);
    num_blocks = ((count + offset - 1) >> sb->s_blocksize) + 1;
    total_blocks = num_blocks;
    start_blk = pos >> sb->s_blocksize_bits;

    if (nova_check_overlap_vmas(sb, sih, start_blk, num_blocks))
    {
        nova_dbgv("COW write overlaps with vma: inode %lu, pgoff %lu, %lu blocks\n",
                  inode->i_ino, start_blk, num_blocks);
        NOVA_STATS_ADD(cow_overlap_mmap, 1);
        try_inplace = 1;
        ret = -EACCES;
        goto out;
    }

    ret = file_remove_privs(filp);
    if (ret)
        goto out;

    inode->i_ctime = inode->i_mtime = current_time(inode);
    time = current_time(inode).tv_sec;

    nova_dbgv("%s: inode %lu, offset %lld, count %lu\n",
              __func__, inode->i_ino, pos, count);

    // 1. allocate as many blocks as this write work need to make a copy
    // 2. append the file write entry
    // 3. update the log tail and the radix
    // 4. finally return the old version of the data to allocator
    epoch_id = nova_get_epoch_id(sb);
    update.tail = sih->log_tail;
    update.alter_tail = sih->alter_log_tail;
    while (num_blocks > 0)
    {
        offset = pos & (nova_inode_blk_size(sih) - 1);
        start_blk = pos >> sb->s_blocksize_bits;

        allocated = nova_new_data_blocks(sb, sih, &blocknr, start_blk,
                                         num_blocks, ALLOC_NO_INIT, ANY_CPU, ALLOC_FROM_HEAD);

        nova_dbg_verbose("%s: alloc %d blocks @ %lu\n",
                         __func__, allocated, blocknr);

        if (allocated <= 0)
        {
            nova_dbg("%s alloc blocks failed %d\n", __func__, allocated);
            ret = allocated;
            goto out;
        }

        step++;
        bytes = sb->s_blocksize * allocated - offset;
        if (bytes > count)
            bytes = count;

        kmem = nova_get_block(inode->i_sb,
                              nova_get_block_off(sb, blocknr, sih->i_blk_type));

        if (offset || ((offset + bytes) & (PAGE_SIZE - 1)) != 0)
        {
            ret = nova_handle_head_tail_blocks(sb, inode, pos, bytes, kmem);
            if (ret)
                goto out;
        }

        // copy kpage to new blocks page by page
        NOVA_START_TIMING(memcpy_w_nvmm_t, memcpy_time);
        nova_memunlock_range(sb, kmem + offset, bytes);
        remain_bytes = bytes;
        page_offset = 0;
        do
        {
            to_copy = remain_bytes < PAGE_SIZE - page_offset ? remain_bytes : PAGE_SIZE - page_offset;

            copied = to_copy -
                     memcpy_to_pmem_nocache(kmem + offset + all_copied,
                                            kaddr[kpage_i] + page_offset, to_copy);

            page_offset = copied & (PAGE_SIZE - 1);
            if (page_offset != 0 || copied == 0)
                nova_warn("An error ocurr when memcpy kpage %d to offset %lu, \
            to_copy: %lu, %lu copied",
                          kpage_i, offset + all_copied, to_copy, copied);
            else
                kpage_i++;

            all_copied += copied;
            remain_bytes -= copied;
        } while (remain_bytes);

        nova_memlock_range(sb, kmem + offset, bytes);
        NOVA_END_TIMING(memcpy_w_nvmm_t, memcpy_time);

        if (data_csum > 0 || data_parity > 0)
        {
            ret = nova_protect_file_data_pages(sb, inode, pos, bytes,
                                               kaddr, blocknr, false);
            if (ret)
                goto out;
        }

        if (pos + all_copied > inode->i_size)
            file_size = cpu_to_le64(pos + all_copied);
        else
            file_size = cpu_to_le64(inode->i_size);

        nova_init_file_write_entry(sb, sih, &entry_data, epoch_id,
                                   start_blk, allocated, blocknr, time, file_size);

        ret = nova_append_file_write_entry(sb, pi, inode, &entry_data, &update);

        if (ret)
        {
            nova_dbg("%s: append inode entry failed\n", __func__);
            ret = -ENOSPC;
            goto out;
        }

        nova_dbg("Write: %p, %lu\n", kmem, all_copied);

        if (all_copied > 0)
        {
            status = all_copied;
            written += all_copied;
            pos += all_copied;
            count -= all_copied;
            num_blocks -= allocated;
        }

        if (unlikely(all_copied != bytes))
        {
            nova_dbg("%s ERROR: %p, bytes %lu, all_copied %lu\n",
                     __func__, kmem, bytes, copied);
            if (status >= 0)
                status = -EFAULT;
        }
        if (status < 0)
            break;

        if (begin_tail == 0)
            begin_tail = update.curr_entry;
    } // while

    data_bits = blk_type_to_shift[sih->i_blk_type];
    sih->i_blocks += (total_blocks << (data_bits - sb->s_blocksize_bits));

    nova_memunlock_inode(sb, pi);
    nova_update_inode(sb, inode, pi, &update, 1);
    nova_memlock_inode(sb, pi);

    ret = nova_reassign_file_tree(sb, sih, begin_tail);
    if (ret)
        goto out;

    inode->i_blocks = sih->i_blocks;

    ret = written;
    NOVA_STATS_ADD(cow_write_breaks, step);
    nova_dbgv("blocks: %lu, %lu\n", inode->i_blocks, sih->i_blocks);

    // *ppos = pos;
    if (pos > inode->i_size)
    {
        i_size_write(inode, pos);
        sih->i_size = pos;
    }

    sih->trans_id++;
out:

    if (ret < 0)
        nova_cleanup_incomplete_write(sb, sih, blocknr, allocated,
                                      begin_tail, update.tail);

    NOVA_END_TIMING(do_cow_write_t, cow_write_time);
    NOVA_STATS_ADD(cow_write_bytes, written);

    if (try_inplace)
        return do_nova_inplace_file_write_async(filp, kaddr, buf, len, ppos, id);

    return ret;
}

/*
@filp: point to file struct
@kaddr: Virtual address corresponding to buffer
@len: length to write
@ppos : offset of this write
@id:for test
*/
ssize_t nova_dax_file_write_async(struct file *filp, struct page **kaddr, void *buf,
                                  size_t len, loff_t ppos, int id)
{
    struct address_space *mapping = filp->f_mapping;
    struct inode *inode = mapping->host;
    int ret;

    if (len == 0)
        return 0;

    if (test_opt(inode->i_sb, DATA_COW))
        ret = do_nova_cow_file_write_async(filp, kaddr, buf, len, ppos, id);
    else
        ret = do_nova_inplace_file_write_async(filp, kaddr, buf, len, ppos, id);

    return ret;
}

/*
@filp: point to file struct
@kaddr: Virtual address corresponding to buffer
@len: length to write
@ppos : offset of this write
@nr_segs: use to calculate;while nr_segs == 0 we can call ki_compete
*/
ssize_t nova_dax_file_read_async(struct file *filp, struct page **kaddr, void *buf,
                                 size_t len, loff_t ppos)
{
    struct inode *inode = filp->f_mapping->host;
    struct super_block *sb = inode->i_sb;
    struct nova_inode_info *si = NOVA_I(inode);
    struct nova_inode_info_header *sih = &si->header;
    struct nova_file_write_entry *entry;
    struct nova_file_write_entry *entryc, entry_copy;
    pgoff_t index, end_index;
    unsigned long offset, page_offset = 0, kpage_i = 0;
    unsigned long copied_once;
    loff_t isize;
    size_t copied = 0, error = -EIO, bytes;
    int rc;
    INIT_TIMING(memcpy_time);

    isize = i_size_read(inode);
    //nova_info("%s : inode size :%lu , ppos : %lu,len %lu\n", __func__, isize, ppos, len);
    if (!isize || ppos > isize)
        goto out;
    if (len <= 0)
        goto out;

    // nova_dbg("%s: inode %lu, offset %lu, len %lu, inode->i_size %lu\n",
    //         __func__, inode->i_ino, ppos, len, isize);

    index = ppos >> PAGE_SHIFT;
    offset = ppos & ~PAGE_MASK;

    if (len > isize - ppos)
        len = isize - ppos;
    entryc = (metadata_csum == 0) ? entry : &entry_copy;
    end_index = (isize - 1) >> PAGE_SHIFT;

    do
    {
        unsigned long nr, left;
        unsigned long nvmm;
        void *dax_mem = NULL;
        int zero = 0;

        if (index >= end_index)
        {
            if (index > end_index)
                goto out;
            nr = ((isize - 1) & ~PAGE_MASK) + 1;
            if (nr <= offset)
                goto out;
        }

        entry = nova_get_write_entry(sb, sih, index);
        if (unlikely(entry == NULL))
        {
            nova_dbgv("Required extent not found: pgoff %lu, inode size %lld\n",
                      index, isize);
            nr = PAGE_SIZE;
            zero = 1;
            goto memcpy;
        }

        if (metadata_csum == 0)
            entryc = entry;
        else if (!nova_verify_entry_csum(sb, entry, entryc))
            return -EIO;

        if (index < entryc->pgoff ||
            index - entryc->pgoff >= entryc->num_pages)
        {
            nova_err(sb, "%s ERROR: %lu, entry pgoff %llu, num %u, blocknr %llu\n",
                     __func__, index, entry->pgoff,
                     entry->num_pages, entry->block >> PAGE_SHIFT);
            return -EINVAL;
        }

        if (entryc->reassigned == 0)
        {
            nr = (entryc->num_pages - (index - entryc->pgoff)) * PAGE_SIZE;
        }
        else
        {
            nr = PAGE_SIZE;
        }
        nvmm = get_nvmm(sb, sih, entryc, index);
        dax_mem = nova_get_block(sb, (nvmm << PAGE_SHIFT));

    memcpy:
        nr = nr - offset;
        if (nr > len - copied)
            nr = len - copied;
        if ((!zero) && (data_csum > 0))
        {
            if (nova_find_pgoff_in_vma(inode, index))
                goto skip_verify;
            if (!nova_verify_data_csum(sb, sih, nvmm, offset, nr))
            {
                nova_err(sb, "%s: nova data checksum and recovery fail! inode %lu, offset %lu, entry pgoff %lu, %u pages, pgoff %lu\n",
                         __func__, inode->i_ino, offset,
                         entry->pgoff, entry->num_pages, index);
                error = -EIO;
                goto out;
            }
        }
    skip_verify:
        //NOVA_START_TIMING(memcpy_r_nvmm_t, memcpy_time);
        page_offset = offset & (PAGE_SIZE - 1);
        if (!zero)
        {
            while (nr > 0)
            {
                if (nr >> PAGE_SHIFT > 0)
                {
                    copied_once = PAGE_SIZE;
                    nr -= PAGE_SIZE;
                }
                else
                {
                    copied_once = nr;
                    nr = 0;
                }
                rc = memcpy_mcsafe(kaddr[kpage_i], dax_mem + offset, copied_once);

                if (rc < 0)
                    goto out;
                copied += copied_once;
                offset += copied_once;
                kpage_i++;
            }
        }
        else
        {
            while (nr > 0)
            {
                if (nr >> PAGE_SHIFT > 0)
                {
                    copied_once = PAGE_SIZE;
                    nr -= PAGE_SIZE;
                }
                else
                {
                    copied_once = nr;
                    nr = 0;
                }

                rc = memset(kaddr[kpage_i], 0, copied_once);
                if (rc < 0)
                    goto out;
                copied += copied_once;
                kpage_i++;
            }
        }
        index = offset >> PAGE_SHIFT;

    } while (copied < len);

out:
    if (filp)
        file_accessed(filp);
    NOVA_STATS_ADD(read_bytes, copied);
    nova_dbg("%s return %zu\n", __func__, copied);
    return copied ? copied : error;
}

int sb_init_wq(struct super_block *sb)
{
    struct workqueue_struct *old;
    struct workqueue_struct *wq = alloc_workqueue("dio/%s",
                                                  WQ_UNBOUND | WQ_MEM_RECLAIM | WQ_HIGHPRI, 4,
                                                  sb->s_id);
    if (!wq)
        return -ENOMEM;

    /*
	 * This has to be atomic as more DIOs can race to create the workqueue
	 */
    old = cmpxchg(&sb->s_dio_done_wq, NULL, wq);
    /* Someone created workqueue before us? Free ours... */
    if (old)
        destroy_workqueue(wq);
    return 0;
}
static void queue_wait_work(struct nova_inode_info *ino_info);

static void kiocb_done(struct kiocb *kiocb,ssize_t ret){

    /*libaio */
    kiocb->ki_complete(kiocb, ret, 0);

    /*io_uring*/
   // struct io_kiocb *req = container_of(kiocb,struct io_kiocb,rw.kiocb);
    // struct io_ring_ctx*ctx = req->ctx;
    // unsigned long flags;
    // spin_lock_irqsave(&ctx->completion_lock,flags);
    
    // spin_unlock_irqrestore(&ctx->completion_lock,flags);

}

void nova_async_work(struct work_struct *p_work)
{
    struct async_work_struct *a_work = container_of(p_work, struct async_work_struct, awork);
    struct file *filp = a_work->a_iocb->ki_filp;
    ssize_t ret = -EINVAL;
    ssize_t written = 0;
    struct iovec *iv = &a_work->my_iov;
    nova_info("start work: %d\n", a_work->id);

    struct pinned_page *pp = (struct pinned_page *)kzalloc(sizeof(struct pinned_page), GFP_KERNEL);
    int j, r;

    unsigned long addr;
    size_t len;
    int n_page;

    down_read(&a_work->tsk->mm->mmap_sem);

    if (!access_ok(iv->iov_base, iv->iov_len))
    {
        goto quit;
    }

    addr = (unsigned long)iv->iov_base;
    len = iv->iov_len + (addr & (PAGE_SIZE - 1));
    addr &= PAGE_MASK;

    n_page = DIV_ROUND_UP(len, PAGE_SIZE);
    pp->num = n_page;
    pp->mapped = 0;
    pp->pages = (struct page **)kzalloc(n_page * sizeof(struct page *), GFP_KERNEL);
    pp->kaddr = (struct page **)kzalloc(n_page * sizeof(struct page *), GFP_KERNEL);

    r = get_user_pages_remote(a_work->tsk, a_work->tsk->mm, addr, n_page, 1, pp->pages, NULL, NULL);

    // ("%s :io_work->id:%d,a_work->my_iov.iov_base: %p , len : %lu , addr : %p , n_page: %d , r :%d",__func__,a_work->id,iv->iov_base,len,addr,n_page,r);

    if (r < pp->num)
        pp->num = r;

    for (j = 0; j < pp->num; j++)
        pp->kaddr[j] = kmap(pp->pages[j]);

    if (iov_iter_rw(&a_work->iter) == READ)
    {
        /*we need to hold a lock*/
        // inode_lock_shared(inode);
        ret = nova_dax_file_read_async(filp, pp->kaddr, a_work->my_iov.iov_base,
                                       a_work->my_iov.iov_len,
                                       a_work->ki_pos);

        // inode_unlock_shared(inode);
        if (ret < 0)
            goto out;
    }

    if (iov_iter_rw(&a_work->iter) == WRITE)
    {
        ret = nova_dax_file_write_async(filp, pp->kaddr, a_work->my_iov.iov_base, a_work->my_iov.iov_len,
                                        a_work->ki_pos, a_work->id);

        if (ret < 0)
            goto out;
    }

    if (iov_iter_rw(&a_work->iter) == READ)
    {
        //set user read buffer pages dirty
        for (j = 0; j < pp->num; j++)
            set_page_dirty(pp->pages[j]);
    }
    nova_info("%s\n",__func__);
out:
    for (j = 0; j < pp->num; j++)
    {
        kunmap(pp->pages[j]);
        put_page(pp->pages[j]);
    }
    kfree(pp->pages);
    kfree(pp->kaddr);

    kfree(pp);
    pp = NULL;

quit:

    up_read(&a_work->tsk->mm->mmap_sem);

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

static void queue_wait_work(struct nova_inode_info *ino_info)

{
    unsigned long wkbit, size;
    struct super_block *sb = ino_info->vfs_inode.i_sb;
    struct list_head *async_pos;
    struct async_work_struct *contn;

    async_pos = ino_info->aio.i_waitque.next;
    while (async_pos != &ino_info->aio.i_waitque)
    {
        contn = container_of(async_pos, struct async_work_struct, aio_waitq);
        wkbit = contn->first_blk;
        size = wkbit + contn->blknr;
        wkbit = find_next_bit(ino_info->aio.work_bitmap, size, wkbit);

        if (wkbit >= size)
        {
            //nova_info("%s :schedu ino->aio.i_waitque: %d\n",__func__,contn->id);
            //nova_info("%s :contn->first_blk:%lu,nr :%lu\n",__func__,contn->first_blk,contn->blknr);
            wkbit = contn->first_blk;
            for_each_clear_bit_from(wkbit, ino_info->aio.work_bitmap, size)
                set_bit(wkbit, ino_info->aio.work_bitmap);
            wkbit = contn->first_blk;
            for_each_set_bit_from(wkbit, ino_info->aio.wait_bitmap, size)
                clear_bit(wkbit, ino_info->aio.wait_bitmap);
            INIT_WORK(&(contn->awork), nova_async_work);
            contn->isQue = queue_work(sb->s_dio_done_wq, &(contn->awork));

            async_pos = async_pos->next;
            list_del(&contn->aio_waitq);
        }
        else
            async_pos = async_pos->next;
    }
}

/*i had not found the same funtion in kernel functions*/
static inline void list_merge(const struct list_head *list, struct list_head *prev, struct list_head *next)
{
    struct list_head *first = list;
    struct list_head *last = list->prev;

    first->prev = prev;
    prev->next = first;

    last->next = next;
    next->prev = last;
}

/*id for test*/
static int id = 0;

ssize_t nova_direct_IO(struct kiocb *iocb, struct iov_iter *iter)
{
    /*for test*/
    //INIT_TIMING(test_start);
    //TEST_START_TIMING("test", test_start);
    /*for test*/

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

                INIT_WORK(&(io_work->awork), nova_async_work);
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
