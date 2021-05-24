#define _GNU_SOURCE /* syscall() is not POSIX */ 
#include<stdlib.h>
#include<string.h>
#include<libaio.h>
#include<errno.h>
#include<stdio.h>
#include<unistd.h>
#include<fcntl.h>
#include<stdbool.h>

/*you should edit also:
    io_nums;
    *p[io_nums] = {&io[0].....&io[io_nums-1]};

*/
#define io_nums 2
#define bs 4096
int main()
{
    
    io_context_t context;
    struct iocb io[io_nums], *p[io_nums];
    for(int i=0;i<io_nums;++i){
        p[i] = &io[i];
    }
    struct io_event e[io_nums];
    unsigned nr_events = io_nums;

    struct timespec timeout;
    timeout.tv_sec = 0;
    timeout.tv_nsec = 10000000;

    /*edit here*/
    char *wbuf_1 = 0, *rbuf_1 = 0;

    int wbuflen = bs;
    int rbuflen = wbuflen+1;

    posix_memalign((void**)&wbuf_1, bs,wbuflen);
    posix_memalign((void**)&rbuf_1,bs,rbuflen);


    memset(wbuf_1, 'b', wbuflen);
    memset(rbuf_1, 0, rbuflen);
   
    /*this is a rule: io_context_t should init with 0*/
    memset(&context, 0, sizeof(io_context_t)); 

    int ret = 0, comp_num = 0, i = 0;

    int fd = open("/mnt/ramdisk/test.txt", O_CREAT|O_RDWR|O_DIRECT, 0644);

    if(fd < 0)
	{
        printf("open file failed ！\n");
        return 0;
    }

    if( 0 != io_setup(nr_events, &context) ){

        printf("io_setup error:%d\n", errno);
        return 0;
    }

    // write
    io_prep_pwrite(&io[0], fd, wbuf_1, wbuflen, 0);
    io_prep_pread(&io[1], fd, rbuf_1, rbuflen-1, 0);

    


    if((ret = io_submit(context,io_nums,p)) != io_nums)
    {
        printf("io_submit error:%d\n", ret);
        io_destroy(context);
        return -1;
    }

    int k=0;
    while(true)
	{
        ret = io_getevents(context,1,1,e,&timeout);
        if(ret == 0){
             printf("comp_num: %d,io_nums : %d,k:%d\n", comp_num,io_nums,++k);

             sleep(1);
             printf("have not done !k = %d\n",++k);
        }
        if(ret < 0)
        {
            printf("io_getevents error:%d\n", ret);
            break;
        }

        if(ret > 0)
        {
            comp_num += ret;
            for( i = 0;i < ret; ++i)
			{
                printf("result,res2:%d, res:%d\n", e[i].res2, e[i].res);
            }
            
        }

        if(comp_num >= io_nums)
		{
            printf("done !\n");
            break;
        }
      
    }
    printf("%s \n\n\n",rbuf_1);

    io_destroy(context);
    return 0;
}