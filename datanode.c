#include "common.h"
#include "galois.h"
#include "arraylist.h"
#include "cuckoo.h"
#include "config.h"

cuckoo* node_id2IPmap = NULL;
char* coord_IP = NULL;
char* cmd=NULL;
char* memory_block = NULL;

typedef struct pipe_task_ptr
{
	pipe_command* pipe_cmd;
	int bid_num;
	
	char** data;
	int** recv_mask;
	int** readencode_mask;
	int** send_mask;
}pipe_task_ptr;

typedef struct parity_task_ptr
{
	parity_command* parity_cmd;
	int* mask;
	char* data;
}parity_task_ptr;



void init_IPs(){
	FILE* ip_file = fopen("nodeip","r");
	coord_IP = (char* )malloc(sizeof(char)*4);
	fscanf(ip_file,"%s\n",coord_IP);

	node_id2IPmap = cuckoo_init(10);
	char* node_id = (char* )malloc(sizeof(char)*4);
	for(int i=0;i<node_num;i++){
		char* ip = (char* )malloc(sizeof(char)*20);
		fscanf(ip_file,"%s %s\n",node_id,ip);
		cuckoo_insert(node_id2IPmap,node_id,(void* )ip);
	}
	free(node_id);
	fclose(ip_file);
}
int delete_len = 0;

int recv_commond(int server_fd){
	//接受所有的cmd字节数据，不反序列化
	int conn_fd = server_accept(server_fd);
	char* ctype = (char* )malloc(sizeof(int));
	recv_bytes(conn_fd,ctype,sizeof(int));
	int type = atoi(ctype);
	int total_len = 0;
	if(type==1){
		total_len = sizeof(pipe_command);
	}else if (type==2||type==3){
		total_len = sizeof(parity_command);
	}else if (type==4){
		char* clen = (char*)malloc(sizeof(int));
		recv_bytes(conn_fd,clen,sizeof(int));
		delete_len = atoi(clen);
		free(clen);
		total_len = sizeof(delete_command)*delete_len;
	}else if (type==5){
		total_len = sizeof(encodenode_command);
	}else if (type==6){
		total_len = sizeof(retrivenode_command);
	}else if(type==7){
		total_len = 1;
	}else{
		perror("unkown cmd type\n");
		exit(1);
	}
	cmd = (char* )malloc(total_len);
	if(type==4){
		for(int i=0;i<delete_len;i++)
			recv_bytes(conn_fd,cmd+i*sizeof(delete_command),sizeof(delete_command));
	}else if(type==7){
		//do nothing
	}else{
		recv_bytes(conn_fd,cmd,total_len);
	}

	free(ctype);
	close(conn_fd);
	return type;
}

//=======================================================================================
//流水线任务cmd相关函数
void commit_pipe_task_ACK(pipe_task_ptr* pipe_task){
	//准备ack
	pipe_ack* ack = (pipe_ack* )malloc(sizeof(pipe_ack));
	ack->type = pipe_task->pipe_cmd->type;
	ack->stripe_id = pipe_task->pipe_cmd->stripe_id;
	ack->pipe_id = pipe_task->pipe_cmd->pipe_id;
	ack->node_id = pipe_task->pipe_cmd->cur_node_id;
	//准备连接CD
	int conn_fd = connect_try(coord_IP,cd_recv_ack_port);
	//发送ack
	size_t need_send = sizeof(pipe_ack);
	char* send_buf = (char*)malloc(sizeof(char)*need_send);
	memcpy(send_buf,ack,need_send);
	send_bytes(conn_fd,send_buf,need_send);

	//free and close
	free(send_buf);
	close(conn_fd);
	free(ack);
}

pipe_task_ptr* init_pipe_task_ptr(pipe_command* pipe_cmd,int bid_num){
	pipe_task_ptr* ptr = (pipe_task_ptr* )malloc(sizeof(pipe_task_ptr));
	ptr->pipe_cmd = pipe_cmd;
	ptr->bid_num = bid_num;
	ptr->data = (char**)malloc(sizeof(char*)*pipe_cmd->run_times);
	ptr->recv_mask = (int**)malloc(sizeof(int*)*pipe_cmd->run_times);
	ptr->readencode_mask = (int**)malloc(sizeof(int*)*pipe_cmd->run_times);
	ptr->send_mask = (int**)malloc(sizeof(int*)*pipe_cmd->run_times);

	for(int i=0;i<pipe_cmd->run_times;i++){
		ptr->data[i] = (char*)malloc(sizeof(char)*file_size);
		ptr->recv_mask[i] = (int*)malloc(sizeof(int)*slice_num);
		ptr->readencode_mask[i] = (int*)malloc(sizeof(int)*slice_num);
		ptr->send_mask[i] = (int*)malloc(sizeof(int)*slice_num);		
	}

	for (int i = 0; i < pipe_cmd->run_times; ++i){
		for(int j = 0; j < slice_num; ++j){
			ptr->recv_mask[i][j] = 0;
			ptr->readencode_mask[i][j] = 0;
			ptr->send_mask[i][j] = 0;
		}
	}
	return ptr;
}

void free_pipe_task_ptr(pipe_task_ptr* ptr){
	for(int i=0; i < ptr->pipe_cmd->run_times; i++){
		free(ptr->data[i]);
		free(ptr->recv_mask[i]);
		free(ptr->readencode_mask[i]);
		free(ptr->send_mask[i]);		
	}
	free(ptr->data);
	free(ptr->recv_mask);
	free(ptr->readencode_mask);
	free(ptr->send_mask);

	free(ptr->pipe_cmd);
	free(ptr);
}

void pipe_recv_data(void* ptr){
	printf("start receive_data\n");
	int slice_size = file_size%slice_num==0?file_size/slice_num:file_size/slice_num+1;
	pipe_task_ptr* pipe_task = (pipe_task_ptr*)ptr;
	int runtime = pipe_task->pipe_cmd->run_times;
	//监听连接
	int pipe_id = pipe_task->pipe_cmd->pipe_id;
	int server_fd = server_init(dn_recv_data_port+pipe_id,10);
	int conn_fd = server_accept(server_fd);

	int recv_index = 0;
	int recv_size = slice_size;
	int run_index = 0;
	while(1){
		if(recv_index==slice_num*runtime)
			break;
		//printf("receive slicenum:%d\n",recv_index);
		if(recv_index%slice_num==slice_num-1)
			recv_size = file_size - slice_size*(recv_index%slice_num);
		else
			recv_size = slice_size;
		run_index = recv_index/slice_num;
		recv_bytes(conn_fd,pipe_task->data[run_index]+(recv_index%slice_num)*slice_size,recv_size);
		pipe_task->recv_mask[run_index][recv_index%slice_num] = 1;
		recv_index++;
	}

	//close and free
	close(conn_fd);
	close(server_fd);
}

void pipe_readencode_data(void* ptr){
	printf("start read and encode data\n");
	int slice_size = file_size%slice_num==0?file_size/slice_num:file_size/slice_num+1;
	pipe_task_ptr* pipe_task = (pipe_task_ptr*)ptr;
	int next_node_id = pipe_task->pipe_cmd->next_node_id;
	int runtime = pipe_task->pipe_cmd->run_times;

	int bid_num = pipe_task->bid_num;

	int* block_files = NULL;	

	if(Data_Type==0){
		block_files = (int* )malloc(sizeof(int)*bid_num);
		for(int i=0;i<bid_num;i++)
			block_files[i] = open("block",O_RDONLY);
	}

	char** tmp_block = (char**)malloc(sizeof(char*)*runtime);
	char** buf = (char** )malloc(sizeof(char*)*runtime);
	for(int i=0;i<runtime;i++){
		tmp_block[i] = (char* )malloc(sizeof(char)*file_size);
		buf[i] = (char* )malloc(sizeof(char)*slice_size);
	}
	char* pse = (char* )malloc(sizeof(char)*slice_size);

	int read_index=0;
	int encode_index=0;
	int read_size = slice_size;
	int encode_size = slice_size;
	int run_index = 0;
	while(1){
		if(read_index < slice_num){
			//printf("read slice_num:%d\n",read_index);
			if(read_index==slice_num-1)
				read_size = file_size - slice_size*read_index;
			else
				read_size = slice_size;
			for (int j = 0; j < bid_num; ++j){
				if(Data_Type==0){
					read_bytes(block_files[j],pse,read_size);
				}else{
					memcpy(pse,memory_block+read_index*slice_size,read_size);
				}
				for(int k = 0; k < runtime; k++){
					galois_w08_region_multiply(pse,rand()%200,read_size,buf[k],0);
					if(j==0)
						memcpy(tmp_block[k]+read_index*slice_size,buf[k],read_size);		
					else
						calc_xor(tmp_block[k]+read_index*slice_size,buf[k],tmp_block[k]+read_index*slice_size,read_size);
				}					
			}
			read_index++;
		}
		
		//增加runtime机制
		if(encode_index%slice_num < read_index){
			if(encode_index==slice_num-1)
				encode_size = file_size - slice_size*(encode_index%slice_num);
			else
				encode_size = slice_size;
			run_index = encode_index/slice_num;
			if(pipe_task->recv_mask[run_index][encode_index%slice_num]==1){
				//计算recv数据和read数据的galois运算结果
				//printf("encode slice_num:%d\n",encode_index);
				calc_xor(tmp_block[run_index]+(encode_index%slice_num)*slice_size,pipe_task->data[run_index]+(encode_index%slice_num)*slice_size,pipe_task->data[run_index]+(encode_index%slice_num)*slice_size,encode_size);
				pipe_task->readencode_mask[run_index][encode_index%slice_num]=1;
				encode_index++;
			}else if(pipe_task->recv_mask[run_index][encode_index%slice_num]==0&&pipe_task->pipe_cmd->last_node_id==-1){
				//如果是头节点，不需要xor直接复制，进行send
				//printf("encode slice_num:%d\n",encode_index);
				memcpy(pipe_task->data[run_index]+(encode_index%slice_num)*slice_size,tmp_block[run_index]+(encode_index%slice_num)*slice_size,encode_size);
				pipe_task->readencode_mask[run_index][encode_index%slice_num]=1;
				encode_index++;
			}else{
				if(read_index==slice_num)
					sleep(0.001);
			}

		}

		if(encode_index==slice_num*runtime)
			break;
	}

	printf("finish send and encode data\n");
	//free and close
	if(Data_Type==0){
		for(int i = 0; i < bid_num; i++)
			close(block_files[i]);
		free(block_files);
	}

	free(pse);
	for (int i = 0; i < runtime; ++i){
		free(tmp_block[i]);
		free(buf[i]);
	}
	free(tmp_block);
	free(buf);
	printf("finish free and close\n");


	//如果是尾节点检查ptr，进行ptr的free
	if(next_node_id==-1){
		//先写校验数据 在发送ack给CD
		for(int i=0;i<runtime;i++){
			if(Data_Type==0){
				FILE* parity_file = fopen("parity","w");
				fwrite(pipe_task->data[i],file_size,1,parity_file);
				fclose(parity_file);
			}else{
				memcpy(memory_block,pipe_task->data[i],file_size);
			}
		}
		commit_pipe_task_ACK(pipe_task);
		printf("finish the commit_pipe_task_ACK\n");
		free_pipe_task_ptr(pipe_task);
		printf("free_pipe_task_ptr\n");
	}

	printf("finish pipe_readencode_data\n");
}

void pipe_send_data(void* ptr){
	int slice_size = file_size%slice_num==0?file_size/slice_num:file_size/slice_num+1;
	pipe_task_ptr* pipe_task = (pipe_task_ptr*)ptr;
	int bid_num = pipe_task->bid_num;
	int runtime = pipe_task->pipe_cmd->run_times;
	//连接下一个节点
	int pipe_id = pipe_task->pipe_cmd->pipe_id;
	int next_node_id = pipe_task->pipe_cmd->next_node_id;
	char* key = itoa(next_node_id);
	char* next_ip = (char*)cuckoo_get(node_id2IPmap,key);
	free(key);
	int conn_fd = connect_try(next_ip,dn_recv_data_port+pipe_id);
	
	//根据mask来传送数据
	int send_index = 0;
	int send_size = slice_size;
	int run_index = 0;
	while(1){
		if(send_index==slice_num*runtime)
			break;
		run_index = send_index/slice_num;
		if((bid_num>0&&pipe_task->readencode_mask[run_index][send_index%slice_num]==1)||(bid_num==0&&pipe_task->recv_mask[run_index][send_index%slice_num]==1)){
			//printf("send slice_num:%d\n",send_index);
			if (send_index%slice_num==slice_num-1)
				send_size = file_size - (send_index%slice_num)*slice_size;
			else
				send_size = slice_size;
			send_bytes(conn_fd,pipe_task->data[run_index]+(send_index%slice_num)*slice_size,send_size);
			pipe_task->send_mask[run_index][send_index%slice_num]=1;
			send_index++;
		}else{
			sleep(0.001);
		}
	}

	//free and close
	close(conn_fd);
	commit_pipe_task_ACK(pipe_task);
	printf("finish commit_pipe_task_ACK\n");
	free_pipe_task_ptr(pipe_task);
	printf("finish free_pipe_task_ptr\n");
}

//======================================================================================
//冗余转移cmd处理相关函数
parity_task_ptr* init_parity_task_ptr(parity_command* parity_cmd){
	parity_task_ptr* ptr = (parity_task_ptr* )malloc(sizeof(parity_task_ptr));
	ptr->parity_cmd = parity_cmd;
	ptr->mask = (int* )malloc(sizeof(int)*slice_num);
	for (int i = 0; i < slice_num; ++i)
		ptr->mask[i] = 0;
	ptr->data = (char* )malloc(sizeof(char)*file_size);
	return ptr;
}


void free_parity_task_ptr(parity_task_ptr* ptr){
	free(ptr->parity_cmd);
	free(ptr->mask);
	free(ptr->data);
	free(ptr);
}

void commit_parity_task_ACK(parity_task_ptr* parity_task){
	parity_ack* ack = (parity_ack* )malloc(sizeof(parity_ack));
	ack->type = parity_task->parity_cmd->type;
	ack->parity_id = parity_task->parity_cmd->parity_id;
	ack->cur_node_id = parity_task->parity_cmd->cur_node_id;

	int conn_fd = connect_try(coord_IP,cd_recv_ack_port);

	int need_send = sizeof(parity_ack);
	char* send_buf = (char* )malloc(need_send);
	memcpy(send_buf,ack,need_send);
	send_bytes(conn_fd,send_buf,need_send);

	free(send_buf);
	close(conn_fd);
	free(ack);
}

void read_parity(void* ptr){
	printf("start thread read_parity\n");
	int slice_size = file_size%slice_num==0?file_size/slice_num:file_size/slice_num+1;
	parity_task_ptr* parity_task = (parity_task_ptr*)ptr;
	FILE* parity_file = NULL;
	if(Data_Type==0) 
		parity_file = fopen("parity","r");

	int read_index = 0;
	int read_size = slice_size;
	while(1){
		///printf("read_index:%d\n",read_index);
		if(read_index == slice_num)
			break;
		if(read_index==slice_num-1)
			read_size = file_size - read_index*slice_size;
		else
			read_size = slice_size;
		if(Data_Type==0)
			fread(parity_task->data+read_index*slice_size,read_size,1,parity_file);
		else
			memcpy(parity_task->data+read_index*slice_size,memory_block+read_index*slice_size,read_size);
		parity_task->mask[read_index] = 1;
		read_index++;
	}

	if(parity_file!=NULL)
		fclose(parity_file);
	printf("end thread read_parity\n");
}

void send_parity(void* ptr){
	printf("start thread send_parity\n");
	int slice_size = file_size%slice_num==0?file_size/slice_num:file_size/slice_num+1;
	parity_task_ptr* parity_task = (parity_task_ptr*)ptr;
	int to_id = parity_task->parity_cmd->node_id;
	char* key = itoa(to_id);
	char* next_ip = (char*)cuckoo_get(node_id2IPmap,key);
	free(key);
	int conn_fd = connect_try(next_ip,dn_recv_data_port);

	int send_index = 0;
	int send_size = slice_size;
	while(1){
		//printf("send_index:%d\n",send_index);
		if(send_index==slice_num)
			break;
		if(send_index<slice_num){
			if(parity_task->mask[send_index]==1){
				if(send_index==slice_num-1)
					send_size = file_size - send_index*slice_size;
				else
					send_size = slice_size;
				send_bytes(conn_fd,parity_task->data+send_index*slice_size,send_size);
				send_index++;
			}else{
				sleep(0.001);
			}
		}

	}
	close(conn_fd);
	commit_parity_task_ACK(parity_task);
	free_parity_task_ptr(parity_task);
	printf("end thread send_parity\n");
}

void recv_parity(void* ptr){
	printf("start thread recv_parity\n");
	int slice_size = file_size%slice_num==0?file_size/slice_num:file_size/slice_num+1;
	parity_task_ptr* parity_task = (parity_task_ptr*)ptr;
	int server_fd = server_init(dn_recv_data_port,10);
	int conn_fd = server_accept(server_fd);

	int recv_index = 0;
	int recv_size = slice_size;
	while(1){
		//printf("recv_index:%d\n",recv_index);
		if(recv_index == slice_num)
			break;

		if(recv_index==slice_num-1)
			recv_size = file_size - recv_index*slice_size;
		else
			recv_size = slice_size;

		recv_bytes(conn_fd,parity_task->data+recv_index*slice_size,recv_size);
		parity_task->mask[recv_index] = 1;
		recv_index++;
	}

	close(conn_fd);
	close(server_fd);
	printf("end thread recv_parity\n");
}

void write_parity(void* ptr){
	printf("start thread write_parity\n");
	int slice_size = file_size%slice_num==0?file_size/slice_num:file_size/slice_num+1;
	parity_task_ptr* parity_task = (parity_task_ptr*)ptr;
	FILE* parity_file = NULL;
	if(Data_Type==0){
		parity_file = fopen("parity","w");
	}

	int write_index = 0;
	int write_size = slice_size;
	while(1){
		//printf("write_index:%d\n",write_index);
		if(write_index==slice_num)
			break;
		if(write_index < slice_num){
			if(parity_task->mask[write_index]==1){
				if(write_index== slice_num-1)
					write_size = file_size - write_index*slice_size;
				else
					write_size = slice_size;
				if(Data_Type==0)
					fwrite(parity_task->data+write_index*slice_size,write_size,1,parity_file);
				else
					memcpy(memory_block+write_index*slice_size,parity_task->data+write_index*slice_size,write_size);
				write_index++;
			}else{
				sleep(0.001);
			}
		}
	}
	if(parity_file!=NULL)
		fclose(parity_file);
	commit_parity_task_ACK(parity_task);
	free_parity_task_ptr(parity_task);
	printf("end thread write_parity\n");
}

//===========================================================================================
//删除副本cmd相关函数
void commit_delete_task_ACK(char* delete_cmds){
	delete_ack* ack = (delete_ack* )malloc(sizeof(delete_ack));
	delete_command* delete_cmd = (delete_command*)malloc(sizeof(delete_command));
	int conn_fd = connect_try(coord_IP,cd_recv_ack_port);
	char* len = itoa(delete_len);
	send_bytes(conn_fd,len,sizeof(int));
	free(len);

	int need_send = sizeof(delete_ack);
	char* send_buf = (char* )malloc(need_send);
	for(int i=0;i<delete_len;i++){
		memcpy(delete_cmd,delete_cmds+i*sizeof(delete_command),sizeof(delete_command));
		ack->type = delete_cmd->type;
		ack->block_id = delete_cmd->block_id;
		ack->node_id = delete_cmd->node_id;
		memcpy(send_buf,ack,need_send);
		send_bytes(conn_fd,send_buf,need_send);
	}
	
	free(send_buf);
	close(conn_fd);
	free(ack);	
}

void delete_replica(void* ptr){
	//删除block
	char* delete_cmds = (char*)ptr;
	delete_command* delete_cmd = (delete_command*)malloc(sizeof(delete_command));

	for(int i=0;i<delete_len;i++){
		memcpy(delete_cmd,delete_cmds+i*sizeof(delete_command),sizeof(delete_command));
		int block_id = delete_cmd->block_id;
		if(Data_Type==0){
			FILE* file = fopen("block","r");
			fclose(file);
		}	
	}
	//commit_ack
	commit_delete_task_ACK(delete_cmds);
	free(delete_cmd);
	free(delete_cmds);
}

//============================central archvial==================================
typedef struct central_task_ptr 
{
	encodenode_command* encodenode_cmd;
	char* local_data;
	int local_block_num;
	int self_is_done;

	int* retrive_block_num;
	int retrive_node_num;
	char** retrive_data;
}central_task_ptr;

typedef struct retrive_data_ptr
{
	int node_index;
	central_task_ptr* ptr;	
}retrive_data_ptr;

central_task_ptr* init_central_task_ptr(encodenode_command* encodenode_cmd){
	central_task_ptr* ptr = (central_task_ptr*)malloc(sizeof(central_task_ptr));
	ptr->encodenode_cmd = encodenode_cmd;
	ptr->self_is_done = 0;
	int count = 0;
	for(int i=0;i<erasure_k;i++){
		if(encodenode_cmd->self_block_ids[i]!=-1)
			count++;
		else
			break;
	}
	if(count>0)
		ptr->local_data = (char*)malloc(file_size*count);
	else
		ptr->local_data = NULL;
	ptr->local_block_num=count;

	count=0;
	for(int i=0;i<erasure_k;i++){
		if(encodenode_cmd->retrive_node_ids[i]!=-1)
			count++;
		else
			break;
	}
	ptr->retrive_node_num = count;
	if(count>0){
		ptr->retrive_data = (char**)malloc(sizeof(char*)*count);
		ptr->retrive_block_num = (int*)malloc(sizeof(int)*count);
		for(int i=0;i<count;i++){
			ptr->retrive_block_num[i] = encodenode_cmd->retrive_block_nums[i];
			ptr->retrive_data[i] = (char*)malloc(file_size*encodenode_cmd->retrive_block_nums[i]);
		}
	}
	return ptr;
}

void free_central_task_ptr(central_task_ptr* ptr){
	if(ptr->local_data!=NULL) free(ptr->local_data);
	if(ptr->retrive_node_num>0){
		for(int i=0;i<ptr->retrive_node_num;i++)
			free(ptr->retrive_data[i]);
		free(ptr->retrive_data);
		free(ptr->retrive_block_num);
	}
	free(ptr->encodenode_cmd);
	free(ptr);
}

void retrivenode_senddata(void* ptr){
	retrivenode_command* retrivenode_cmd = (retrivenode_command*)ptr;
	int encode_id = retrivenode_cmd->encode_id;
	char* sendbuf = malloc(file_size);
	char* key = itoa(encode_id);
	char* target_ip = (char*)cuckoo_get(node_id2IPmap,key);
	int conn_fd = connect_try(target_ip,dn_recv_data_port+retrivenode_cmd->node_id);
	for(int i=0;i<erasure_k;i++){
		if(retrivenode_cmd->retrive_block_ids[i]!=-1){
			if(Data_Type==0){
				FILE* file = fopen("block","r");
				fread(sendbuf,file_size,1,file);
				fclose(file);
			}else{
				memcpy(sendbuf,memory_block,file_size);
			}
			send_bytes(conn_fd,sendbuf,file_size);
		}
	}
	free(key);
	free(sendbuf);
	free(retrivenode_cmd);
}

void encodenode_readdata(void* ptr){
	printf("start thread encodenode_readdata\n");
	central_task_ptr* central_task = (central_task_ptr*)ptr;
	//printf("local_block_num:%d\n",central_task->local_block_num);
	for(int i=0;i<central_task->local_block_num;i++){
			int block_id = central_task->encodenode_cmd->self_block_ids[i];
			//printf("block_id:%d\n",block_id);
			if (Data_Type==0){
				FILE* file = fopen("block","r");
				fread(central_task->local_data+i*file_size,file_size,1,file);
				fclose(file);	
			}else{
				memcpy(central_task->local_data+i*file_size,memory_block,file_size);
			}
	}
	central_task->self_is_done = 1;
	printf("finish thread encodenode_readdata\n");
}

void retrive_data_thread(void* ptr){
	retrive_data_ptr* retrive_ptr = (retrive_data_ptr*)ptr;
	int idx = retrive_ptr->node_index;
	printf("start retrive_data_thread index:%d\n",idx);
	int node_id = retrive_ptr->ptr->encodenode_cmd->retrive_node_ids[idx];
	int server_fd = server_init(dn_recv_data_port+node_id,10);
	int conn_fd = server_accept(server_fd);
	for(int i=0;i<retrive_ptr->ptr->retrive_block_num[idx];i++)
		recv_bytes(conn_fd,retrive_ptr->ptr->retrive_data[idx]+file_size*i,file_size);
	close(conn_fd);
	close(server_fd);
	free(retrive_ptr); 
}

void encodenode_retrivedata(void* ptr){
	printf("start thread encodenode_retrivedata\n");
	//向n个datanode retrive data 然后检查self_is_done==1之后编码
	central_task_ptr* central_task = (central_task_ptr*)ptr;
	encodenode_command* encodenode_cmd = central_task->encodenode_cmd;

	pthread_t* threads = NULL;
	retrive_data_ptr** retrive_data_ptrs = NULL; 
	if(central_task->retrive_node_num>0){
		retrive_data_ptrs = (retrive_data_ptr**)malloc(sizeof(retrive_data_ptr*)*central_task->retrive_node_num);
		threads = (pthread_t*)malloc(sizeof(pthread_t)*central_task->retrive_node_num);
	}
	printf("retrive_node_num:%d\n",central_task->retrive_node_num);
	for(int i=0;i<central_task->retrive_node_num;i++){
		retrive_data_ptrs[i] = (retrive_data_ptr*)malloc(sizeof(retrive_data_ptr));
		retrive_data_ptrs[i]->node_index = i;
		retrive_data_ptrs[i]->ptr = central_task;
		pthread_create(&threads[i],NULL,(void*)&retrive_data_thread,(void*)retrive_data_ptrs[i]);
	}
	for (int i = 0; i < central_task->retrive_node_num; ++i)
		pthread_join(threads[i],NULL);
	//编码

	while(1){
		if(central_task->self_is_done ==1){
			break;
		}else{
			sleep(0.001);
		}
	}

	char* parity = (char*)malloc(file_size);
	char* buf = (char*)malloc(file_size);
	for(int i=0;i<erasure_r;i++){
		int is_first = 1;
		for(int j = 0; j < central_task->local_block_num; j++){
			galois_w08_region_multiply(central_task->local_data+j*file_size,rand()%200,file_size,buf,0);
			if (is_first==1){
				is_first==0;
				memcpy(parity,buf,file_size);
			}else{
				calc_xor(parity,buf,parity,file_size);
			}
		}

		for(int j=0;j<central_task->retrive_node_num;j++){
			for(int k=0;k<central_task->retrive_block_num[j];k++){
				galois_w08_region_multiply(central_task->retrive_data[j]+file_size*k,rand()%200,file_size,buf,0);
				if (is_first==1){
					is_first==0;
					memcpy(parity,buf,file_size);
				}else{
					calc_xor(parity,buf,parity,file_size);
				}		
			}
		}

		if(Data_Type==0){
			FILE* file = fopen("parity","w");
			fwrite(parity,file_size,1,file);
			fclose(file);
		}else{
			memcpy(memory_block,parity,file_size);
		}
	}
	free(parity);
	free(buf);

	//发送ack给cd
	printf("send encodenode_ack to coordinator\n");
	encodenode_ack* ack = (encodenode_ack*)malloc(sizeof(encodenode_ack));
	ack->type = encodenode_cmd->type;
	ack->stripe_id = encodenode_cmd->stripe_id;
	ack->encode_id = encodenode_cmd->encode_id;
	int conn_fd = connect_try(coord_IP,cd_recv_ack_port);
	int sendlen = sizeof(encodenode_ack);
	char* sendbuf = malloc(sendlen);
	memcpy(sendbuf,ack,sendlen);
	send_bytes(conn_fd,sendbuf,sendlen);
	free(sendbuf);
	close(conn_fd);
	free(ack);
	free_central_task_ptr(central_task);
	printf("finish thread encodenode_retrivedata\n");
}

void init_memory_block(){
	memory_block = (char*)malloc(sizeof(char)*file_size);
	for(int i=0;i<file_size;i++){
		memory_block[i] = rand()%128;
	}
}
//===========================================================================================
int main(int argc, char const *argv[])
{
	//初始化环境
	init_IPs();
	init_memory_block();
	//初始化变量
	int cmd_type;
	int server_fd = server_init(dn_recv_cmd_port,30);
	while(1){
		//接受cmd
		cmd_type = recv_commond(server_fd);
		printf("recv cmd_type:%d\n",cmd_type);
		//解析cmd 处理cmd，多线程
		if (cmd_type==1){
			pipe_command* pipe_cmd = (pipe_command*)malloc(sizeof(pipe_command));
			memcpy(pipe_cmd,cmd,sizeof(pipe_command));
			free(cmd);

			int recv_id = pipe_cmd->last_node_id;
			int send_id = pipe_cmd->next_node_id;
			int bid_num = 0;//;
			for(int i=0;i<erasure_k;i++){
				if(pipe_cmd->block_ids[i]!=-1) 
					bid_num++;
			}
			printf("recv_id:%d , send_id:%d , bid_num=%d.\n",recv_id,send_id,bid_num);

			pipe_task_ptr* ptr = init_pipe_task_ptr(pipe_cmd,bid_num);

			//创建一个接受线程
			if(recv_id!=-1){
				printf("create thread for receiving data\n");
				pthread_t pthread_recv_data;
				pthread_create(&pthread_recv_data,NULL,(void* )&pipe_recv_data,(void* )ptr);
			}

			if(bid_num>0){
				printf("create thread for reading and encoding data\n");
				pthread_t pthread_readencode_data;
				pthread_create(&pthread_readencode_data,NULL,(void* )&pipe_readencode_data,(void* )ptr);
			}

			//创建一个发送线程
			if (send_id!=-1){
				printf("create thread for sending data\n");
				pthread_t pthread_send_data;
				pthread_create(&pthread_send_data,NULL,(void* )&pipe_send_data,(void* )ptr);
			}

		}else if(cmd_type==2){
			parity_command* parity_cmd = (parity_command*)malloc(sizeof(parity_command));
			memcpy(parity_cmd,cmd,sizeof(parity_command));
			free(cmd);
			parity_task_ptr* ptr = init_parity_task_ptr(parity_cmd);	
			pthread_t pthread_read_parity;
			pthread_t pthread_send_parity;
			pthread_create(&pthread_read_parity,NULL,(void* )&read_parity,(void* )ptr);
			pthread_create(&pthread_send_parity,NULL,(void* )&send_parity,(void* )ptr);

		}else if(cmd_type==3){
			parity_command* parity_cmd = (parity_command*)malloc(sizeof(parity_command));
			memcpy(parity_cmd,cmd,sizeof(parity_command));
			free(cmd);
			parity_task_ptr* ptr = init_parity_task_ptr(parity_cmd);
			pthread_t pthread_recv_parity;
			pthread_t pthread_write_parity;
			pthread_create(&pthread_recv_parity,NULL,(void* )&recv_parity,(void* )ptr);
			pthread_create(&pthread_write_parity,NULL,(void*)&write_parity,(void* )ptr);						

		}else if(cmd_type==4){
			pthread_t pthread_delete_repica;
			pthread_create(&pthread_delete_repica,NULL,(void* )&delete_replica,(void*)cmd);

		}else if (cmd_type==5){
			encodenode_command* encodenode_cmd = (encodenode_command*)malloc(sizeof(encodenode_command));
			memcpy(encodenode_cmd,cmd,sizeof(encodenode_command));
			free(cmd);
			central_task_ptr* ptr = init_central_task_ptr(encodenode_cmd);
			//一个读取本地block并计算线程
			pthread_t pthread_readdata;
			pthread_create(&pthread_readdata,NULL,(void* )&encodenode_readdata,(void*)ptr);
			//一个收集其他节点中block线程
			pthread_t pthread_retrivedata;
			pthread_create(&pthread_retrivedata,NULL,(void* )&encodenode_retrivedata,(void*)ptr);
		}else if (cmd_type==6){
			retrivenode_command* retrivenode_cmd = (retrivenode_command*)malloc(sizeof(retrivenode_command));
			memcpy(retrivenode_cmd,cmd,sizeof(retrivenode_command));
			free(cmd);

			pthread_t pthread_senddata2encode;
			pthread_create(&pthread_senddata2encode,NULL,(void*)&retrivenode_senddata,(void*)retrivenode_cmd);

		}else if (cmd_type==7){
			break;
		}else{
			perror("unkown cmdtype");
		}
	}
	//free and close
	close(server_fd);
	free(memory_block);
	free(coord_IP);
	cuckoo_destroy(node_id2IPmap,1);
	return 0;
}



