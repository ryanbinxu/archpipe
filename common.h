#ifndef _COMMON_H
#define _COMMON_H
#include <assert.h>

#include <time.h>
#include <sys/time.h>    //gettimeofday 
#include <sys/stat.h>    //定义在打开和创建文件时用到的一些符号常量
#include <unistd.h>      // linux  read  write
#include <string.h>
#include <stdio.h>       // linux input output 
#include <stdlib.h>      //standard library 
#include <stdint.h>
#include <signal.h>      
#include <math.h>
#include <malloc.h>
#include <fcntl.h>       //定义 open,fcntl函数原型
#include <error.h>  
#include <stdbool.h>

#include <sys/types.h>    //off_t
#include <sys/socket.h>   //socket ,blind ,listen ,accept,...
#include <netinet/in.h>   //sockaddr_in ,struct sockaddr,...
#include <arpa/inet.h>    //inet_addr ,inet_ntoa ,htonl,...
#include <net/if.h>       // struct ifreq ,struct ifconf
#include <sys/ioctl.h>    //setsockopt
#include <pthread.h>      //pthread_create
#include <netinet/tcp.h>  
#include <sys/resource.h>

#include "config.h"

typedef struct retrivenode_command
{
	int type;//6
	int stripe_id;
	int encode_id;
	int node_id;
	int retrive_block_ids[erasure_k];
}retrivenode_command;

typedef struct encodenode_command
{
	int type;//5
	int stripe_id;
	int encode_id;
	int retrive_node_ids[erasure_k];
	int retrive_block_nums[erasure_k];
	int self_block_ids[erasure_k];
}encodenode_command;

typedef struct encodenode_ack
{
	int type;
	int stripe_id;
	int encode_id;
}encodenode_ack;


typedef struct pipe_command
{
	int type;//1 means pipe_task
	int pipe_id;
	int stripe_id;
	int next_node_id;
	int cur_node_id;
	int last_node_id;
	int block_ids[erasure_k];
	int run_times;
}pipe_command;

typedef struct pipe_ack
{
	int type;//1
	int stripe_id;
	int pipe_id;
	int node_id;
}pipe_ack;

typedef struct parity_command
{
	int type;//2->send 3->recv
	int parity_id;
	int node_id;
	int cur_node_id;
}parity_command;

typedef struct parity_ack
{
	int type;//2/3
	int parity_id;
	int cur_node_id;
}parity_ack;

typedef struct delete_command
{
	int type; //4
	int block_id;
	int node_id;
}delete_command;

typedef struct delete_ack
{
	int type;//4
	int block_id;
	int node_id;
}delete_ack;

int connect_try(char *ip,int port);//尝试连接目标server，返回connectfd或者-1
int server_init(int port,int backlog);//初始化服务器用于接受信息
int server_accept(int listen_sock);//服务器端bind listen以及accept

ssize_t read_bytes(int fd,char *buf,size_t nbytes);    //一次读文件
ssize_t write_bytes(int fd,char *buf,size_t nbytes);   //一次写文件
ssize_t send_bytes(int sockfd,char *buf,size_t nbytes);//一次性发送
ssize_t recv_bytes(int sockfd,char *buf,size_t nbytes);//一次性接收
void calc_xor(char *d1,char * d2,char *out,size_t len);//纠删码中加运算即亦或运算


char* itoa(int num);
double compute_Time(double bandwidth,int edge_num,int run_time);

#endif