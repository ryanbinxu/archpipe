#include "common.h"

/**
 * [connect_try description]   client尝试连接server
 * @param  ip   [description]  服务端ip 地址
 * @param  port [description]  服务端端口号
 * @return      [description]  返回套接字sockfd
 */
int connect_try(char *ip,int port){//支持重连接
    printf("connect_try begin..................................\n");
    int nsec,sockfd;
    struct sockaddr_in sa;   //地址
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    sa.sin_addr.s_addr=inet_addr(ip);

    sockfd=socket(AF_INET,SOCK_STREAM,0);  //socket 套接字

    if((-1 != sockfd))
    {
        for(nsec=1;nsec <= Max_Wait;nsec++)
        {
            if(0 == connect(sockfd,(struct sockaddr *)&sa,sizeof(sa))){  //客户端建立连接，若成功返回0，若出错返回-1

            int sendsize = 51200000;
            int svcsize=51200000;
            if(setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, (char*)&sendsize, sizeof(sendsize)) < 0){//设置发送缓冲区大小
            perror("setsockopt");
            exit(1);
            }
            if(setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, (char*)&svcsize, sizeof(svcsize)) < 0){ //设置接收缓冲区大小
            perror("setsockopt");
            exit(1);
            }
            int flag = 1;   
            if(setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (char *)&flag, sizeof(int)) < 0){//设置端口复用
            perror("setsockopt");
            exit(1);
            }
            if(setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (char *) &flag, sizeof(int)) < 0){//屏蔽Nagle算法，避免不可预测的延迟
            perror("setsockopt");
            exit(1);
            }
             
            printf("connect %s succeed\n", ip);  //连接成功
            return sockfd;//连接Server 
            }
            printf("connect_try %s sleep ........\n",ip);
            if(nsec <= Max_Wait) sleep(1);
        }
        close(sockfd);
    }
    return -1;
}

/**
* 创建服务器 bind and listen
  * @param  port    [description]   服务端端口号
 *  @param  backlog [description]   socket可排队的最大连接个数
 *  @return         [description]    服务器套接字
*/
int server_init(int port,int backlog){
    int listen_sock;//创建监听套接字
    int opt = 1;
    struct sockaddr_in sa;  //地址
    socklen_t slen = sizeof(sa);  //协议地址的长度
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_ANY);
    sa.sin_port = htons(port);

    listen_sock=socket(AF_INET, SOCK_STREAM, 0); //套接字建立域  套接字类型 0
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));//SO_REUSEADDR 仅仅表示可以重用本地本地地址、本地端口 
    bind(listen_sock, (struct sockaddr *)&sa, sizeof(sa));  //将套接字与地址绑定  套接字  地址 地址长度 
   //服务端监听 成功返回0  出错返回-1 。c语言规定，任何非0的数像1 -1等都被认为是真，而0被认为是假
    if (listen(listen_sock, backlog)<0){  
        printf("portListen ~ listen() error.\n");
        return -1;
    }
    return listen_sock;
}

/**
 * [server_accept description]      服务端建立连接  server
 * listen_sock 服务器套接字
 * return 连接套接字
 */
int server_accept(int listen_sock){
    printf("server_accept begin .........................................\n");
    int connect_sock;//创建通信套接字
    struct sockaddr_in sa;  //地址
    socklen_t slen = sizeof(sa);

    while (1){
        connect_sock = accept(listen_sock, (struct sockaddr *)&sa, &slen);  //通信套接字 成功返回套接字 出错返回-1
        if (-1 == connect_sock){
            printf("portListen ~ accept() error.\n");
        }else{
            printf("server_accept made .client ip ：%s.\n", inet_ntoa(sa.sin_addr));
            break;
        }
    }
    int sendsize=512000;
    int svcsize=512000;
    if(setsockopt(connect_sock, SOL_SOCKET, SO_RCVBUF, (char*)&sendsize, sizeof(sendsize)) < 0){//设置发送缓冲区大小
        perror("setsockopt");
        exit(1);
    }
    if(setsockopt(connect_sock, SOL_SOCKET, SO_SNDBUF, (char*)&svcsize, sizeof(svcsize)) < 0){ //设置接收缓冲区大小
        perror("setsockopt");
        exit(1);
    }
    int flag = 1;   
    if(setsockopt(connect_sock, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(int)) < 0){//设置端口复用
        perror("setsockopt");
        exit(1);
    }
    if(setsockopt(connect_sock, IPPROTO_TCP, TCP_NODELAY, (char *) &flag, sizeof(int)) < 0){//屏蔽Nagle算法，避免不可预测的延迟
        perror("setsockopt");
        exit(1);
    }
    return connect_sock;  //返回通信套接字  成功返回套接字 出错返回-1
}

/**
 * [read_bytes description]         一次读文件
 * @param  fd     [description]      套接字
 * @param  buf    [description]     字节缓冲区
 * @param  nbytes [description]     要读取的文件总数
 * @return        [description]     返回读取的文件总数
 */
ssize_t read_bytes(int fd,char *buf,size_t nbytes){
    size_t total=0;
    int once;
    for(once=0;total<nbytes;total+=once){
        once = read(fd,buf+total,nbytes-total);  //返回值大于0,表示读了部分或者是全部的数据
        if(once<=0)break;
    }
    return total;
}

/**
 * [write_bytes description]
 * @param  fd     [description]
 * @param  buf    [description]
 * @param  nbytes [description]
 * @return        [description]
 */
ssize_t write_bytes(int fd,char *buf,size_t nbytes){//一次写文件
    size_t total=0;
    int once;
    for(once=0;total<nbytes;total+=once){
        once = write(fd,buf+total,nbytes-total);  //返回值大于0,表示写了部分或者是全部的数据
        if(once<=0)break;
    }
    return total;
}

/**
 * [send_bytes description]       一次性发送文件
 * @param  sockfd [description]   套接字
 * @param  buf    [description]   缓冲区
 * @param  nbytes [description]   字节大小
 * @return        [description]   总的发送文件大小
 */
ssize_t send_bytes(int sockfd,char *buf,size_t nbytes){
    size_t total=0;
    int once;
    for(once=0;total<nbytes;total+=once){
        once=send(sockfd,buf+total,nbytes-total,0);
        if(once<=0)break;
    }
    return total;
}

/**
 * [recv_bytes description]      一次性接收文件
 * @param  sockfd [description]
 * @param  buf    [description]
 * @param  nbytes [description]
 * @return        [description]
 */
ssize_t recv_bytes(int sockfd,char *buf,size_t nbytes){
    size_t total=0;
    int once;
    for(once=0;total<nbytes;total+=once){
        once=recv(sockfd,buf+total,nbytes-total,0);
        //printf("recv_bytes_once:%d\n", once);
        if(once<=0)break;
    }
    //printf("recv_bytes_total:%d\n", total);
    return total;
}

/**
 * [calc_xor description]    XOR模拟纠删码  *************??????*********
 * @param d1  [description]  数据块D1
 * @param d2  [description]  数据块D2
 * @param out [description]  XOR运算后输出的数据块
 * @param len [description]
 */
void calc_xor(char *d1,char * d2,char *out,size_t len){
    size_t i,j;
    long *pd1=(long *)d1;
    long *pd2=(long *)d2;
    long *plout=(long *)out;
    if(!d1 || !d2 || !out)return;
    for(i=0; i < len/sizeof(long);i++) *(plout+i)= *(pd1+i)^ *(pd2+i);
    if(0 != len%sizeof(long)){
        for(j=i*sizeof(long);j<len;j++)  *(out+i)= *(d1+j) ^ *(d2+j);
    }
}



char* itoa(int num){
    char* blockID=(char*)malloc(sizeof(char)*20);
    memset(blockID,'\0',20);
    sprintf(blockID, "%d", num);
    return blockID;
}

double compute_Time(double bandwidth,int edge_num,int run_time){
    if (edge_num==0){
        return 0;
    }
    double block_size = file_size/erasure_k*8; //bit
    double res = block_size/bandwidth*(run_time+edge_num/slice_num);
    return res;
}