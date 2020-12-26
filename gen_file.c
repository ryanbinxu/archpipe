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

#include <sys/types.h>    //off_t
#include <sys/socket.h>   //socket ,blind ,listen ,accept,...
#include <netinet/in.h>   //sockaddr_in ,struct sockaddr,...
#include <arpa/inet.h>    //inet_addr ,inet_ntoa ,htonl,...
#include <net/if.h>       // struct ifreq ,struct ifconf
#include <sys/ioctl.h>    //setsockopt
#include <netinet/tcp.h>  
#include <sys/resource.h>

int main(int argc, char const *argv[])
{
	if (argc!=2)
	{
		perror("error params");
	}
	int size = atoi(argv[1]);
	FILE* file = fopen("block","a+");
	for(int i=0;i<size*1024*1024;i++){
		fprintf(file,"%c",rand()%128);
	}
	fclose(file);
	return 0;
}