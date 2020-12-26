#include "arraylist.h"
#include "cuckoo.h"
#include "common.h"
#include "config.h"
#include "stripecons.h"
#include "placement.h"

typedef struct archpipe_sches
{
	int stripe_id;
	int pipe_num;
	int* pipe_ids;
	int** pipe_nodeids;
	int*** pipe_blockids;
	int* pipe_runtimes;
}archpipe_sches;

typedef struct archcentral_sches
{
	int stripe_id;
	int encode_id;
	int* central_nodeids;
	int** central_blockids;
}archcentral_sches;

typedef struct edge
{
	int from_id;
	int to_id;
	double bandwidth;
}edge;

typedef struct path
{
	edge** edges;
	int edge_num;
	double bottleneck; //max_node_num = erasure_k=> max_edge_num = erasure_k-1
}path;

int* placement = NULL;
double** network = NULL;
int** used_times = NULL;
int** stripe_cons = NULL;

cuckoo* node_idmap2block_ids = NULL;//图中节点信息
Array_List* edgeset = NULL;//图中边信息
Array_List* node_list = NULL;

double schedule_time = 0.0;
double encoding_time = 0.0;
double parity_time = 0.0;
double delete_time = 0.0;

void free_edgeset(){
	//printf("%d\n",edgeset->length);
	for (int i = 0; i < edgeset->length; ++i){
		//printf("%d\n",i);
		edge* e = (edge*)Array_List_GetAt(edgeset,i);
		//printf("from_id:%d,to_id:%d\n",e->from_id,e->to_id);
		if (e!=NULL)
			free(e);
		//printf("%d\n",i);
	}
	Array_List_Free(edgeset);
}

void display_edgeset(){
	printf("%lu\n",edgeset->length);
	for (int i = 0; i < edgeset->length; ++i){
		printf("%dth edge: ",i);	
		edge* e = (edge*)Array_List_GetAt(edgeset,i);
		printf("from_id:%d,to_id:%d\n",e->from_id,e->to_id);
	}
}

void free_node_idmap2block_ids(){
	for (int i = 0; i < node_idmap2block_ids->cap; i++){
        if (node_idmap2block_ids->nodes[i].taken){
            free(node_idmap2block_ids->nodes[i].key);
            Array_List_Free((Array_List* )node_idmap2block_ids->nodes[i].val);
            
        }
    }
    free(node_idmap2block_ids->nodes);
    free(node_idmap2block_ids);
}

void free_path(path* path){
	if (path!=NULL){
		if (path->edges!=NULL)
			free(path->edges);
		free(path);
	}
}

void display_sches(archpipe_sches* sches){
	printf("the pipeline sches is constructed for stripe_id:%d\n", sches->stripe_id);
	printf("pipeline num is %d\n",sches->pipe_num);
	printf("================================\n");
	for (int i = 0; i < sches->pipe_num; ++i){
		printf("the %dth pipeline detail\n", i);
		printf("pipeline id:%d\n", sches->pipe_ids[i]);
		printf("pipeline runtimes:%d\n",sches->pipe_runtimes[i]);
		printf("nodeids and blockids:\n");
		double min_bw = 2000;
		for (int j = 0; j < erasure_k; ++j){
			if(sches->pipe_nodeids[i][j]==-1)
				break;
			printf("node_id:%d---->block_ids:", sches->pipe_nodeids[i][j]);
			if(j<erasure_k-1&&sches->pipe_nodeids[i][j+1]!=-1){
				printf("%f    ",network[sches->pipe_nodeids[i][j]][sches->pipe_nodeids[i][j+1]]);
				int from_id = sches->pipe_nodeids[i][j]; 
				int to_id = sches->pipe_nodeids[i][j+1];
				if(network[from_id][to_id]/(used_times[from_id][to_id]-1)<min_bw){
					min_bw= network[from_id][to_id]/(used_times[from_id][to_id]-1);
				}
			}	
			for (int k = 0; k < erasure_k; ++k){
				if (sches->pipe_blockids[i][j][k]==-1)
					break;
				printf("%d  ",sches->pipe_blockids[i][j][k]);
			}

			//可删除
			//printf("/// ");
			//char* key = itoa(sches->pipe_nodeids[i][j]);
			//Array_List* list = (Array_List*)cuckoo_get(node_idmap2block_ids,key); 
			//free(key);
			//for (int m = 0; m < list->length; ++m){
			//	printf("%d ", (int)Array_List_GetAt(list,m));
			//}
			printf(".....\n");
		}
		printf("min_bw:%f\n",min_bw);
		printf("================================\n");
	}
	printf("================================\n");
}

void init_used_times(){
	for (int i = 0; i < node_num; ++i){
		for (int j = 0; j < node_num; ++j){
			used_times[i][j] = 1;
		}
	}
}

path* add_p2path(path* p1,path* p2,int is_head){
	path* new_path = (path*)malloc(sizeof(path));
	new_path->edges = (edge**)malloc(sizeof(edge*)*(erasure_k-1));
	new_path->edge_num = p1->edge_num+p2->edge_num;
	new_path->bottleneck = p1->bottleneck<p2->bottleneck?p1->bottleneck:p2->bottleneck;
	edge* e = NULL;
	if (is_head==0){
		int i=0;
		for ( i = 0; i < p1->edge_num; ++i){
			new_path->edges[i] = p1->edges[i];
		}
		int len = i;
		for(int j=0;j<p2->edge_num;i++,j++){
			for(int k=0;k<len;k++){
				if (new_path->edges[k]->from_id==p2->edges[j]->to_id){
					free_path(new_path);
					return NULL;
				}
			}
			new_path->edges[i] = p2->edges[j];
		}
	}else{
		int i=new_path->edge_num-1;
		for (int j = p1->edge_num-1; j >=0 ; j--,i--){
			new_path->edges[i] = p1->edges[j];
		}		
		int len = i;
		for(int j=p1->edge_num-1;j>=0;i--,j--){
			for (int k = len+1; k <new_path->edge_num; ++k){
				if(new_path->edges[k]->to_id==p1->edges[j]->from_id){
					free_path(new_path);
					return NULL;
				}
			}
			new_path->edges[i] = p2->edges[j];
		}
	}
	return new_path;

}

path* add_e2path(path* p, edge* e,int is_head){
	path* new_path = (path*)malloc(sizeof(path));
	new_path->edges = (edge**)malloc(sizeof(edge*)*(erasure_k-1));
	new_path->edge_num = 0;
	new_path->bottleneck = p->bottleneck;

	if (is_head==1){
		for (int i = p->edge_num-1; i >= 0 ; i--){
			new_path->edges[i+1] = p->edges[i];
			if(e->from_id==p->edges[i]->to_id){//存在环，直接返回null
				free(new_path->edges);
				free(new_path);
				return NULL;
			}
		}
		new_path->edges[0] = e;
		new_path->edge_num = p->edge_num+1;		
	}else{
		for (int i = 0; i < p->edge_num; ++i){
			new_path->edges[i] = p->edges[i];
			if (e->to_id==p->edges[i]->from_id){
				free(new_path->edges);
				free(new_path);
				return NULL;
			}
		}
		new_path->edges[p->edge_num] = e;
		new_path->edge_num = p->edge_num+1;
	}
	if(e->bandwidth<p->bottleneck){
		new_path->bottleneck = e->bandwidth;
	}
	return new_path;
}

void display_path(path* path){
	printf("check_path: ");
	for(int i=0;i<path->edge_num;i++){
		printf("%d->", path->edges[i]->from_id);
	}
	printf("%d\n",path->edges[path->edge_num-1]->to_id);
}

bool is_matchpipe(path* check_path){
	//display_path(check_path);
	//检查当前的path是否包含所有的blocks
	Array_List* check_list = Array_List_Init();
	int last_node = -1;
	char* key = NULL;
	for (int i = 0; i <= check_path->edge_num; ++i)
	{
		int node_id = -1;
		if (i==check_path->edge_num){
			node_id = check_path->edges[i-1]->to_id;
		}else{
			node_id = check_path->edges[i]->from_id;
		}
		
		if(last_node==node_id)
			continue;
		last_node = node_id;

		key = itoa(node_id);
		Array_List* block_list = cuckoo_get(node_idmap2block_ids,key);
		free(key);
		for (int j = 0; j < block_list->length; ++j){
			int is_exist = 0;
			int bid = (int)Array_List_GetAt(block_list,j);
			for (int m = 0; m < check_list->length; ++m){
				if((int)Array_List_GetAt(check_list,m)==bid){
					is_exist = 1;
					break;
				}	
			}
			if (is_exist==0){
				Array_List_Insert(check_list,(void*)bid,-1);
			}
		}
	}
	bool ret = false;
	if (check_list->length==erasure_k){
		ret = true;
	}
	//free blocklist
	Array_List_Free(check_list);
	return ret;
}

void insert_sort_list(Array_List* ready_paths,path* new_path){
	bool is_insert = false;
	for (int i = 0; i < ready_paths->length; ++i){
		path* tp = (path*)Array_List_GetAt(ready_paths,i);
		if (new_path->bottleneck > tp->bottleneck||(new_path->bottleneck == tp->bottleneck&&new_path->edge_num<tp->edge_num)){
			Array_List_Insert(ready_paths,(void* )new_path,i);
			is_insert = true;
		 	break;
		 } 
	}
	if(!is_insert){
		Array_List_Insert(ready_paths,(void*)new_path,-1);
	}
}

path* compare_paths(path* p1,path* p2){
	if (p1!=NULL&&p2!=NULL)
	{
		if (p1->bottleneck>p2->bottleneck)
		return p1;
	else if(p1->bottleneck<p2->bottleneck)
		return p2;
	else
		return p1->edge_num<=p2->edge_num?p1:p2;	
	}else{
		if(p1==NULL&&p2!=NULL)
			return p2;
		else if(p1!=NULL&&p2==NULL)
			return p1;
		else
			return NULL;
	}
}

//new to create
edge* get_the_biggest_edge(){
	edge* res = NULL;
	int idx = -1;
	double bw = 0;
	for(int i=0;i<edgeset->length;i++){
		edge* e = (edge*)Array_List_GetAt(edgeset,i);
		if(e->bandwidth>bw){
			bw = e->bandwidth;
			res = e;
			idx = i;
		}
	}
	Array_List_RemoveAt(edgeset,idx);
	return res;
}

//fix  get the most edge from set
path* dpsearch_optimalpipe(int stripe_id,double mutiple_bw_limit){
	//按照降序从edgeset中抽取edge组成pipe,找到满足条件的pipe
	Array_List* ready_paths = Array_List_Init();
	path* res_path = NULL;
	Array_List* tmp_paths = Array_List_Init();

	for (int i = 0; i < edgeset->length; ++i){
		if (res_path!=NULL) break;
		edge* tmp_edge = (edge*)Array_List_GetAt(edgeset,i);
		//edge* tmp_edge = get_the_biggest_edge();
		if(tmp_edge->bandwidth < mutiple_bw_limit) break;
		for(int k = 0;k < tmp_paths->length; k++){
			path* add_path = (path*)Array_List_GetAt(tmp_paths,k);
			//insert_sort_list(ready_paths,add_path);
			Array_List_Insert(ready_paths,(void*)add_path,-1);
		}
		Array_List_Clear(tmp_paths);

		for (int j = 0; j < ready_paths->length; ++j){
			path* tmp_path = (path*)Array_List_GetAt(ready_paths,j);
			int path_head_id = tmp_path->edges[0]->from_id;
			int path_tail_id = tmp_path->edges[tmp_path->edge_num-1]->to_id;
			path* new_path = NULL;
			if(tmp_edge->from_id == path_tail_id){
				new_path = add_e2path(tmp_path,tmp_edge,0);
			}

			if(new_path!=NULL){
				if(is_matchpipe(new_path)){
					res_path = compare_paths(res_path,new_path);
				}else{
					if(new_path->edge_num<erasure_k-1){
						Array_List_Insert(tmp_paths,(void*)new_path,-1);
						for (int k = 0; k < ready_paths->length; ++k){
							path* tail_path = (path*)Array_List_GetAt(ready_paths,k);
							if(tail_path->edges[0]->from_id == tmp_edge->to_id){
								if(tail_path->edge_num+new_path->edge_num<=erasure_k-1){
									path* nn_path = add_p2path(new_path,tail_path,0);
									if(nn_path!=NULL){
										if(is_matchpipe(nn_path)){
											res_path = compare_paths(nn_path,res_path);
											}else{
												if (nn_path->edge_num<erasure_k-1){
													Array_List_Insert(tmp_paths,(void*)nn_path,-1);											
												}else{
													free_path(nn_path);
												}
											}
										}
								}
							}
						}
					}else{
						free_path(new_path);
					}

				}
			}

	}

		path* new_path = (path*)malloc(sizeof(path));
		new_path->edges = (edge**)malloc(sizeof(edge*)*(erasure_k-1));
		new_path->edge_num = 0;
		new_path->edges[new_path->edge_num] = tmp_edge;
		new_path->bottleneck = tmp_edge->bandwidth;
		new_path->edge_num++;
		if(is_matchpipe(new_path)){
			res_path = compare_paths(res_path,new_path);
			//return new_path;
		}else{
			if (new_path->edge_num < erasure_k-1){
				Array_List_Insert(tmp_paths,(void*)new_path,-1);
				for (int j = 0; j < ready_paths->length; ++j){
					path* tmp_path = (path*)Array_List_GetAt(ready_paths,j);
					if (tmp_edge->to_id == tmp_path->edges[0]->from_id){
						path* n_path = add_e2path(tmp_path,tmp_edge,1);
						if (n_path!=NULL){
							if (is_matchpipe(n_path)){
								res_path = compare_paths(res_path,n_path);
							}else{
								if(n_path->edge_num<erasure_k-1){
									Array_List_Insert(tmp_paths,(void*)n_path,-1);
								}else{
									free_path(n_path);
								}
							}
						}
					}				
				}		
			}else{
				free_path(new_path);
			}
		}
	}

	for(int i = 0;i<ready_paths->length;i++){
		free_path((path*)Array_List_GetAt(ready_paths,i));
	}
	for(int i = 0;i<tmp_paths->length;i++){
		free_path((path*)Array_List_GetAt(tmp_paths,i));
	}

	Array_List_Free(ready_paths);
	Array_List_Free(tmp_paths);
	return res_path;
}

void init_nodeid2blocksmap(int* placement,int stripe_id){
	//初始化所有的节点信息
	char* tmp_key = NULL;
	//printf("init node info\n");
	for (int j = 0; j < erasure_k; ++j){
			int block_id = stripe_cons[stripe_id][j];
			for (int i = 0; i < rep_num; ++i){
				int node_id = placement[block_id*rep_num+i];
				tmp_key = itoa(node_id);
				int isExists = cuckoo_exists(node_idmap2block_ids,tmp_key);
				if (isExists==1){
					Array_List* list = cuckoo_get(node_idmap2block_ids,tmp_key);
					Array_List_Insert(list,(void* )block_id,-1);	
				}else{
					Array_List* newlist = Array_List_Init();
					Array_List_Insert(newlist,(void* )block_id,-1);
					cuckoo_insert(node_idmap2block_ids,tmp_key,(void* )newlist);
				}
				free(tmp_key);
			}
	}
}

//fix  no need to sort
void sort_edges(int* placement,double** network,int stripe_id){
	char* tmp_key = NULL;
	//printf("init edge info\n");
	//初始化所有的边信息
	//并且按照降序存储在edgeset中
	Array_List* node_list = Array_List_Init();
	for (int i = 0; i < node_idmap2block_ids->cap; i++){
        if (node_idmap2block_ids->nodes[i].taken){
            int node_id = atoi(node_idmap2block_ids->nodes[i].key);
            //printf("%d\n", node_id);
            Array_List_Insert(node_list, (void*)node_id,-1);
        }
    }

    //printf("node_list.length = %d\n", node_list->length);

    for (int i = 0; i < node_list->length; ++i){
    	for (int j = 0; j < node_list->length; ++j){
    		int node_id1 = (int)Array_List_GetAt(node_list,i); 
    		int node_id2 = (int)Array_List_GetAt(node_list,j);
    		//printf("node_id1=%d,node_id2=%d\n",node_id1,node_id2);
    		if (node_id1==node_id2){
    			tmp_key = itoa(node_id1);
    			Array_List* list = (Array_List*)cuckoo_get(node_idmap2block_ids,tmp_key);
    			free(tmp_key);
    			if (list->length!=erasure_k)
    				continue;
    		}
    		edge* new_edge = (edge* )malloc(sizeof(edge));
    		new_edge->from_id = node_id1;
    		new_edge->to_id = node_id2;
    		new_edge->bandwidth = network[node_id1][node_id2];

			int idx=0;
			for (idx = 0; idx < edgeset->length; ++idx){
				edge* tmp_edge = (edge*)Array_List_GetAt(edgeset,idx);
				if(new_edge->bandwidth>tmp_edge->bandwidth)
					break;
			}
			Array_List_Insert(edgeset,(void*)new_edge,idx);
			//Array_List_Insert(edgeset,(void*)new_edge,-1);
    	}
    }
    Array_List_Free(node_list);
} 

void free_archpipe_sches(archpipe_sches* sches){
	if (sches!=NULL){
		if(sches->pipe_ids!=NULL)
			free(sches->pipe_ids);
		for (int i = 0; i < sches->pipe_num; ++i){
			if(sches->pipe_nodeids[i]!=NULL)
				free(sches->pipe_nodeids[i]);
			for(int j = 0; j < erasure_k; j++){
				if(sches->pipe_blockids[i][j]!=NULL)
					free(sches->pipe_blockids[i][j]);
			}
			if(sches->pipe_blockids[i]!=NULL)
				free(sches->pipe_blockids[i]);
		}
		if(sches->pipe_nodeids!=NULL)
			free(sches->pipe_nodeids);
		if(sches->pipe_blockids!=NULL)
			free(sches->pipe_blockids);
		free(sches);
	}
}

archpipe_sches* init_archpipe_sches(int pipe_num){
	archpipe_sches* sches =  (archpipe_sches*)malloc(sizeof(archpipe_sches));
	sches->pipe_ids = (int* )malloc(sizeof(int)*sches->pipe_num);
	sches->pipe_nodeids = (int**)malloc(sizeof(int*)*sches->pipe_num);
	sches->pipe_blockids = (int***)malloc(sizeof(int*)*sches->pipe_num);
	sches->pipe_num = pipe_num;
	for (int i = 0; i < pipe_num; ++i){
		sches->pipe_nodeids[i] = (int* )malloc(sizeof(int)*erasure_k);//k个数据分块组成一个条带
		sches->pipe_blockids[i] = (int** )malloc(sizeof(int*)*erasure_k);
		for (int j = 0; j < erasure_k; ++j){
			sches->pipe_blockids[i][j] = (int* )malloc(sizeof(int)*erasure_k);
		}
	}
	for (int i = 0; i < pipe_num; ++i){
		for (int j = 0; j < erasure_k; ++j){
			sches->pipe_nodeids[i][j] = -1;
			for (int k = 0; k < erasure_k; ++k){
				sches->pipe_blockids[i][j][k] = -1;	
			}
			
		}
	}
	sches->pipe_runtimes = (int* )malloc(sizeof(int)*sches->pipe_num);
}

void swap(int* array, int i,int j){
	int tmp = array[j];
	array[j] = array[i];
	array[i] = tmp;
}

double cur_bottleneck = 0;
int* res_bid_array = NULL;
int* res_nid_array = NULL;
int cur_pipe_length = erasure_k;

void process_the_array(int* bid_array,int* nid_array,int index){
	if(index==erasure_k){
		//for(int i=0;i<erasure_k;i++)
		///	printf("%d",nid_array[i]);
		//printf("\n");

		//记录最大瓶颈的序列
        double bottleneck = 1002;
        int nodenum = 1;
		for(int i = 0;i<erasure_k-1;i++){
			int from_id = nid_array[i];
			int to_id = nid_array[i+1];
			if(from_id!=to_id) nodenum++;
			double bandwidth = network[from_id][to_id]/used_times[from_id][to_id];
			if (bandwidth < bottleneck)
				bottleneck = bandwidth;
/*			if (bottleneck < cur_bottleneck)
				break;*/
		}
		//printf("bottleneck:%lf, nodenum:%d,cur_bottleneck:%lf\n", bottleneck,nodenum,cur_bottleneck);
		if(bottleneck>cur_bottleneck||(bottleneck==cur_bottleneck&&cur_pipe_length>nodenum)){
			cur_bottleneck = bottleneck;
			cur_pipe_length = nodenum;
			for(int i= 0;i < erasure_k; i++){
				//printf("%d",nid_array[i]);
				res_nid_array[i] = nid_array[i];
				res_bid_array[i] = bid_array[i]; 
			}
			//printf("\n");
		}
		return;
	}

	int bid = bid_array[index];
	for(int i=0;i<rep_num;i++){
		int nid = placement[bid*rep_num+i];
		nid_array[index] = nid;
		process_the_array(bid_array,nid_array,index+1);
	}
	
}

void rankRuc(int* bid_array,int index){
	if(index==erasure_k){  
        int* nid_array = (int* )malloc(sizeof(int)*erasure_k);
        process_the_array(bid_array,nid_array,0);  
        free(nid_array);
        return ;  
    }  	
	for (int i=index;i<erasure_k;++i){ 
		swap(bid_array,i,index); 
		rankRuc(bid_array,index+1);   
		swap(bid_array,i,index); 
	}
}

archpipe_sches* force_schedule_single_archpipe(int* placement,double** network,int stripe_id){
	//此函数采用暴力方法构建单条流水线
	archpipe_sches* sches =  init_archpipe_sches(1);
	init_used_times();
	sches->stripe_id  = stripe_id;
	sches->pipe_runtimes[0] = erasure_r;
	sches->pipe_ids[0] = 0;
	//algorithm start:
	//全排列所有的数据分块id
	res_nid_array = (int* )malloc(sizeof(int)*erasure_k);
	res_bid_array = (int* )malloc(sizeof(int)*erasure_k);
	int* bid_array = (int* )malloc(sizeof(int)*erasure_k);
	for (int i = 0; i < erasure_k; ++i)
		bid_array[i] = stripe_cons[stripe_id][i];
		//bid_array[i] = stripe_id*erasure_k+i;

	cur_bottleneck = 0;
	cur_pipe_length = erasure_k;

	rankRuc(bid_array,0);
	//printf("end rankRuc\n");

	int last_id = -1;
	int nodecount = -1;
	int blockcount = 0;
	for (int i = 0; i < erasure_k; ++i){
		int node_id = res_nid_array[i];
		//printf("node_id:%d\n",node_id);
		if (last_id!=node_id){
			last_id=node_id;
			nodecount++;
			blockcount=0;
			sches->pipe_nodeids[0][nodecount] = node_id;
			sches->pipe_blockids[0][nodecount][blockcount] = res_bid_array[i];
		}else{
			blockcount++;
			sches->pipe_blockids[0][nodecount][blockcount] = res_bid_array[i];
		}
	}

	for(int i=0;i<erasure_k-1;++i){
		if(sches->pipe_nodeids[0][i]!=-1&&sches->pipe_nodeids[0][i+1]!=-1){
			int from_id = sches->pipe_nodeids[0][i];
			int to_id = sches->pipe_nodeids[0][i+1];
			used_times[from_id][to_id]++;
		}
	}

	free(bid_array);
	free(res_nid_array);
	free(res_bid_array);
	return sches;
}

archpipe_sches* force_schedule_mutiple_archpipe(int* placement,double** network,int stripe_id){
	int res_path_num = 0;
	int res_runtimes = 0;
	if(erasure_r==1||erasure_r==2||erasure_r==3){
		res_path_num = erasure_r;
		res_runtimes = 1;
	}else{
		if (erasure_r%2==0){
			res_path_num = 2;
			res_runtimes = erasure_r/2;	
		}
		if (erasure_r%3==0){
			res_path_num = 3;
			res_runtimes = erasure_r/3;	
		}
	}

	archpipe_sches* sches =  init_archpipe_sches(res_path_num);
	init_used_times();
	sches->stripe_id  = stripe_id;

	res_nid_array = (int* )malloc(sizeof(int)*erasure_k);
	res_bid_array = (int* )malloc(sizeof(int)*erasure_k);
	int* bid_array = (int* )malloc(sizeof(int)*erasure_k);
	for (int i = 0; i < erasure_k; ++i)
		bid_array[i] = stripe_cons[stripe_id][i];
		//bid_array[i] = stripe_id*erasure_k+i;

	for(int i=0;i<res_path_num;i++){
		cur_bottleneck = 0;
		cur_pipe_length = erasure_k;
		//start recursive
		rankRuc(bid_array,0);
		//load box
		int last_id = -1;
		int nodecount = -1;
		int blockcount = 0;

		sches->pipe_runtimes[i] = res_runtimes;
		sches->pipe_ids[i] = i;
		for (int j = 0; j < erasure_k; ++j){
			int node_id = res_nid_array[j];
			if (last_id!=node_id){
				last_id=node_id;
				nodecount++;
				blockcount=0;
				sches->pipe_nodeids[i][nodecount] = node_id;
				sches->pipe_blockids[i][nodecount][blockcount] = res_bid_array[j];
			}else{
				blockcount++;
				sches->pipe_blockids[i][nodecount][blockcount] = res_bid_array[j];
			}
		}
		//update
		for(int j=0;j<erasure_k-1;++j){
			if(sches->pipe_nodeids[i][j]!=-1&&sches->pipe_nodeids[i][j+1]!=-1){
				int from_id = sches->pipe_nodeids[i][j];
				int to_id = sches->pipe_nodeids[i][j+1];
				used_times[from_id][to_id]++;
			}
		}
		//finish
	}

	free(res_nid_array);
	free(res_bid_array);
	free(bid_array);
	return sches;
}

void convert_path2sches(archpipe_sches* sches,path* res_path,int pipe_no){
	int last_id = -1;
	Array_List* tmp_list = Array_List_Init();
	char* key = NULL;
	int nodecount = 0;
	int blockcount = 0;
	for (int i = 0; i <= res_path->edge_num; ++i)
	{
		int node_id = -1;
		if (i==res_path->edge_num){
			node_id = res_path->edges[i-1]->to_id;
		}else{
			node_id = res_path->edges[i]->from_id;
		}
		if(node_id==last_id)
			continue;
		last_id = node_id;
		sches->pipe_nodeids[pipe_no][nodecount] = node_id;

		key = itoa(node_id);
		Array_List* block_ids = (Array_List*)cuckoo_get(node_idmap2block_ids,key);
		free(key);		

		for (int j = 0; j < block_ids->length; ++j){
			int bid  = (int)Array_List_GetAt(block_ids,j);
			bool is_exist = false;
			for(int k = 0; k < tmp_list->length; k++){
				if(bid==(int)Array_List_GetAt(tmp_list,k)){
					is_exist = true;
					break;
				}
			}
			if (!is_exist){
				sches->pipe_blockids[pipe_no][nodecount][blockcount] = bid;
				Array_List_Insert(tmp_list,(void*)bid,-1);
				blockcount++;
			}
		}
		nodecount++;
		blockcount=0;
	}
	Array_List_Free(tmp_list);
}

//double dp_single_archpipe_time = 0.0;
//double prepare_time = 0.0;
archpipe_sches* dp_schedule_single_archpipe(int* placement,double** network,int stripe_id){
	//此函数中算法为单条流水线构建
	//struct timeval bg_tm, ed_tm;
	//gettimeofday(&bg_tm,NULL);
	archpipe_sches* sches =  init_archpipe_sches(1);
	sches->stripe_id  = stripe_id;
	sches->pipe_runtimes[0] = erasure_r;
	sches->pipe_ids[0] = 0;

	//algorithm start:
	//数据块存储信息index {stripe_id*erasure_k*rep_num~(stripe_id+1)*erasure_k*rep_num}
	edgeset = Array_List_Init();
	node_idmap2block_ids = cuckoo_init(10);
	init_used_times();
	//printf("sort_edges start.\n");

	init_nodeid2blocksmap(placement,stripe_id);
	sort_edges(placement,network,stripe_id);
	//gettimeofday(&ed_tm,NULL);
	//double time = ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000; 
	//prepare_time=  prepare_time+time;

	//gettimeofday(&bg_tm,NULL);
	//printf("dpsearch_optimalpipe start.\n");
	path* res_path = dpsearch_optimalpipe(stripe_id,0);
	//printf("dpsearch_optimalpipe finish.\n");
	//装箱
	for(int i = 0;i<res_path->edge_num;i++){
		int from_id = res_path->edges[i]->from_id;
		int to_id = res_path->edges[i]->to_id;
		used_times[from_id][to_id]++;
	}

	convert_path2sches(sches,res_path,0);
	free_edgeset();
	free_node_idmap2block_ids();
	//printf("schedule_single_archpipe finish.\n");
	//gettimeofday(&ed_tm,NULL);
	//double time = ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000;
	//dp_single_archpipe_time = dp_single_archpipe_time+time;
	return sches;
}

void update_environment(path* path){
	for (int i = 0; i < path->edge_num; ++i)
	{
		int from_id = path->edges[i]->from_id;
		int to_id = path->edges[i]->to_id;
		used_times[from_id][to_id]++;
		//network[from_id][to_id] = network[from_id][to_id]/used_times[from_id][to_id];

		if(from_id==to_id)
			break;

		edge* update_e = NULL;
		for (int idx = 0; idx < edgeset->length; ++idx){
			update_e = (edge*)Array_List_GetAt(edgeset,idx);
			if(update_e->from_id == from_id&&update_e->to_id == to_id){
				update_e->bandwidth = network[from_id][to_id]/used_times[from_id][to_id]; 
				Array_List_RemoveAt(edgeset,idx);
				break;
			}
		}

		for (int idx = 0; idx < edgeset->length; ++idx){
			edge* e = (edge*)Array_List_GetAt(edgeset,idx);
			if (e->bandwidth < update_e->bandwidth){
				Array_List_Insert(edgeset,(void* )update_e,idx);
				break;
			}
		}
	}
}

//利用单条流水线构建函数，更新network表，来规划多条流水线
archpipe_sches* dp_schedule_mutiple_archpipe(int* placement,double** network,int stripe_id){
	//根据数据块位置信息以及网络带宽信息进行流水线规划，算法为文章核心
	//此函数中算法为多条流水线构建
	int res_path_num = 0;
	int res_runtimes = 0;
	if(erasure_r==1||erasure_r==2||erasure_r==3){
		res_path_num = erasure_r;
		res_runtimes = 1;
	}else{
		if (erasure_r%2==0){
			res_path_num = 2;
			res_runtimes = erasure_r/2;	
		}
		if (erasure_r%3==0){
			res_path_num = 3;
			res_runtimes = erasure_r/3;	
		}
	}

	archpipe_sches* sches =  init_archpipe_sches(res_path_num);
	init_used_times();
	sches->stripe_id  = stripe_id;
	path** res_path_array = (path** )malloc(sizeof(path*)*res_path_num);
	edgeset = Array_List_Init();
	node_idmap2block_ids = cuckoo_init(10);
	double mutiple_bw_limit = 0.0;

	//printf("sort_edges start.\n");
	init_nodeid2blocksmap(placement,stripe_id);
	sort_edges(placement,network,stripe_id);
	
	for(int i= 0;i<res_path_num;i++){
		//更新network-table 以及edgeset的排序
		path* res_path = dpsearch_optimalpipe(stripe_id,mutiple_bw_limit);
		if(i==0)
			mutiple_bw_limit = res_path->bottleneck/res_path_num;
		res_path_array[i] = res_path;
		sches->pipe_runtimes[i] = res_runtimes;
		sches->pipe_ids[i] = i;
		update_environment(res_path_array[i]);
	}

	for (int i = 0; i < res_path_num; ++i){
		convert_path2sches(sches,res_path_array[i],i);
	}
	
	free_edgeset();
	free_node_idmap2block_ids();
	return sches;
}

bool isSameZone(int id1,int id2){
	int zone1 = id1/(node_num/zone_num);
	int zone2 = id2/(node_num/zone_num);
	if (zone1==zone2)
		return true;
	else
		return false;
}

archpipe_sches* random_schedule_single_archpipe(int* placement,int stripe_id){
	archpipe_sches* sches = init_archpipe_sches(1);
	init_used_times();
	sches->stripe_id  = stripe_id;
	sches->pipe_runtimes[0] = erasure_r;
	sches->pipe_ids[0] = 0;
	srand(time(0));

	int* res_node_ids = (int*)malloc(sizeof(int)*erasure_k);
	int* res_block_ids = (int*)malloc(sizeof(int)*erasure_k);
	int* block_ids = (int*)malloc(sizeof(int)*erasure_k);
	for(int i = 0; i < erasure_k; i++)
		block_ids[i] = stripe_cons[stripe_id][i];
		//block_ids[i] = stripe_id*erasure_k + i;

	int last_id = -1;
	for(int i = 0; i < erasure_k; i++){
		//随机一个block
		int block_idx = rand()%(erasure_k-i);
		int block_id = block_ids[block_idx]; 
		//随机一个node
		int retry = 0;
		while(1){
			int node_idx = rand()%rep_num;
			int node_id = placement[block_id*rep_num+node_idx];
			//检查是否构成环，不构成继续，，否则替换另一个node
			last_id = -1;
			for(int j = i-1; j >=0; j--){
				if(res_node_ids[j] == node_id){
					last_id = j;
					break;
				}
			}
			if(last_id==-1||last_id==i-1){
				res_node_ids[i] = node_id;
				res_block_ids[i] = block_id;
				break;
			}
			retry++;
			if(retry==20){
				i--;
				break;
			}
		}
		swap(block_ids,block_idx,erasure_k-1-i);
	}
	last_id = -1;
	int nodecount = -1;
	int blockcount = 0;
	for (int i = 0; i < erasure_k; ++i){
		int node_id = res_node_ids[i];
		if (last_id!=node_id){
			last_id=node_id;
			nodecount++;
			blockcount=0;
			sches->pipe_nodeids[0][nodecount] = node_id;
			sches->pipe_blockids[0][nodecount][blockcount] = res_block_ids[i];
		}else{
			blockcount++;
			sches->pipe_blockids[0][nodecount][blockcount] = res_block_ids[i];
		}
	}

	for(int i=0;i<erasure_k-1;++i){
		if(sches->pipe_nodeids[0][i]!=-1&&sches->pipe_nodeids[0][i+1]!=-1){
			int from_id = sches->pipe_nodeids[0][i];
			int to_id = sches->pipe_nodeids[0][i+1];
			used_times[from_id][to_id]++;
		}
	}
	free(block_ids);
	free(res_node_ids);
	free(res_block_ids);
	return sches;
}


archpipe_sches* random_schedule_mutiple_archpipe(int* placement,int stripe_id){
	int res_path_num = 0;
	int res_runtimes = 0;
	if(erasure_r==1||erasure_r==2||erasure_r==3){
		res_path_num = erasure_r;
		res_runtimes = 1;
	}else{
		if (erasure_r%2==0){
			res_path_num = 2;
			res_runtimes = erasure_r/2;	
		}
		if (erasure_r%3==0){
			res_path_num = 3;
			res_runtimes = erasure_r/3;	
		}
	}
	archpipe_sches* sches = init_archpipe_sches(res_path_num);
	init_used_times();
	sches->stripe_id  = stripe_id;
	int* block_ids = (int*)malloc(sizeof(int)*erasure_k);
	for(int i = 0; i < erasure_k; i++)
		block_ids[i] = stripe_cons[stripe_id][i];
	srand(time(0));
	//todo
	for (int p = 0; p < res_path_num; ++p)
	{
		sches->pipe_ids[p]=p;
		sches->pipe_runtimes[p]=res_runtimes;
		int* res_node_ids = (int*)malloc(sizeof(int)*erasure_k);
		int* res_block_ids = (int*)malloc(sizeof(int)*erasure_k);
		
		int last_id = -1;
		for(int i = 0; i < erasure_k; i++){
			//随机一个block
			int block_idx = rand()%(erasure_k-i);
			int block_id = block_ids[block_idx]; 
			//随机一个node
			int retry = 0;
			while(1){
				int node_idx = rand()%rep_num;
				int node_id = placement[block_id*rep_num+node_idx];
				//检查是否构成环，不构成继续，，否则替换另一个node
				last_id = -1;
				for(int j = i-1; j >=0; j--){
					if(res_node_ids[j] == node_id){
						last_id = j;
						break;
					}
				}
				if(last_id==-1||last_id==i-1){
					res_node_ids[i] = node_id;
					res_block_ids[i] = block_id;
					break;
				}
				retry++;
				if(retry==20){
					i--;
					break;
				}
			}
			swap(block_ids,block_idx,erasure_k-1-i);
		}
		last_id = -1;
		int nodecount = -1;
		int blockcount = 0;
		for (int i = 0; i < erasure_k; ++i){
			int node_id = res_node_ids[i];
			if (last_id!=node_id){
				last_id=node_id;
				nodecount++;
				blockcount=0;
				sches->pipe_nodeids[p][nodecount] = node_id;
				sches->pipe_blockids[p][nodecount][blockcount] = res_block_ids[i];
			}else{
				blockcount++;
				sches->pipe_blockids[p][nodecount][blockcount] = res_block_ids[i];
			}
		}

		for(int i=0;i<erasure_k-1;++i){
			if(sches->pipe_nodeids[p][i]!=-1&&sches->pipe_nodeids[p][i+1]!=-1){
				int from_id = sches->pipe_nodeids[p][i];
				int to_id = sches->pipe_nodeids[p][i+1];
				used_times[from_id][to_id]++;
			}
		}


		for (int i = 0; i < erasure_k; ++i)
		{
			printf("%d ",res_node_ids[i]);
		}
		printf("\n");

		free(res_node_ids);
		free(res_block_ids);
	}

	free(block_ids);
	return sches;
}

archpipe_sches* schedule_pipe_arch(int type,int stripe_id,int is_force){
	archpipe_sches* sches = NULL;
	struct timeval bg_tm, ed_tm;
	gettimeofday(&bg_tm,NULL);
	//type=1 --->random single pipe
	//type=2 --->network-aware single pipe
	//type=3 --->network-aware mutiple pipe
	if(type==1){
		sches = random_schedule_single_archpipe(placement,stripe_id);
	}else if(type==2){
		if(is_force==1){
			sches = force_schedule_single_archpipe(placement,network,stripe_id);
		}else{
			sches= dp_schedule_single_archpipe(placement,network,stripe_id);
		}
	}else if(type==3){
		if(is_force==1){
			sches = force_schedule_mutiple_archpipe(placement,network,stripe_id);
		}else{
			sches = dp_schedule_mutiple_archpipe(placement,network,stripe_id);
		}
	}else if(type==4){
		sches = random_schedule_mutiple_archpipe(placement,stripe_id);
	}else{
		perror("unknow schedule_pipe type");
	}
	gettimeofday(&ed_tm,NULL);
	double once_time = ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000;
	schedule_time = schedule_time + once_time;
	//display_sches(sches);
	printf("stripeid:%d the process of pipeline schedule has done\n",stripe_id);
	return sches;
}

//==============================================================================
//=======================以下属于pipe网络函数========================================
cuckoo* node_id2IPmap = NULL;
char* coord_IP = NULL;
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

void prepare_pipecmds(archpipe_sches* sches,Array_List* pipe_cmds){
	for (int i = 0; i < sches->pipe_num; ++i){
		for (int j = 0; j < erasure_k; ++j){
			int cur_id = sches->pipe_nodeids[i][j];
			int last_id = j==0?-1:sches->pipe_nodeids[i][j-1];
			int next_id = j==erasure_k-1?-1:sches->pipe_nodeids[i][j+1];
			pipe_command* pipe_cmd = NULL;
			if (cur_id!=-1){
				pipe_cmd = (pipe_command* )malloc(sizeof(pipe_command));
				pipe_cmd->type = 1;
				pipe_cmd->stripe_id = sches->stripe_id;
				pipe_cmd->pipe_id = sches->pipe_ids[i];
				pipe_cmd->last_node_id = last_id;
				pipe_cmd->next_node_id = next_id;
				pipe_cmd->cur_node_id = cur_id;
				pipe_cmd->run_times = sches->pipe_runtimes[i];
				for (int k = 0; k < erasure_k; ++k){
					int cur_bid = sches->pipe_blockids[i][j][k];
					pipe_cmd->block_ids[k] = cur_bid; 	
				}
			}else{
				break;
			}
			Array_List_Insert(pipe_cmds,(void* )pipe_cmd,-1);
		}
	}
}

int choose_bestparitynode(int from_id,Array_List* parity_nodelist){
	double max_nw = 0;
	int res_to_id = -1;
	int index = -1;
	for(int i=0;i<parity_nodelist->length;i++){
		int to_id = (int)Array_List_GetAt(parity_nodelist,i);
		if(network[from_id][to_id]>max_nw){
			res_to_id = to_id;
			max_nw = network[from_id][to_id];
			index = i;
		}
	}
	if(index!=-1){
		Array_List_RemoveAt(parity_nodelist,index);
	}
	return res_to_id;
}

int parity_id = 0;
void prepare_relocate_paritycmds(archpipe_sches* sches,Array_List* parity_cmds){
	Array_List* parity_nodelist = Array_List_Init();
	parity_id = 0;
	for(int i = 0; i<node_num;i++)
		Array_List_Insert(parity_nodelist,(void*)i,-1);

	cuckoo* last_id2parity_num_map = cuckoo_init(10);
	for(int i=0;i<sches->pipe_num;i++){
		int last_id = sches->pipe_nodeids[i][erasure_k-1];
		for(int j=0;j<erasure_k;j++){
			if(sches->pipe_nodeids[i][j]==-1){
				last_id = sches->pipe_nodeids[i][j-1];
				break;
			}
		}

		char* key = itoa(last_id);
		if(cuckoo_exists(last_id2parity_num_map,key)==1){
			int times = (int)cuckoo_get(last_id2parity_num_map,key);
			times = times + sches->pipe_runtimes[i];
			cuckoo_insert(last_id2parity_num_map,key,(void* )times);
		}else{
			Array_List_RemoveData(parity_nodelist,(void*)last_id);
			cuckoo_insert(last_id2parity_num_map,key,(void* )sches->pipe_runtimes[i]);
		}
		free(key);
	}

	for (int i = 0; i < last_id2parity_num_map->cap; i++)
	{
        if (last_id2parity_num_map->nodes[i].taken)
        {
        	int from_id = atoi(last_id2parity_num_map->nodes[i].key);
            int times = (int)last_id2parity_num_map->nodes[i].val;
            for(int j=0;j<times-1;j++)
            {
            	int to_id = choose_bestparitynode(from_id,parity_nodelist);
            	if(to_id!=-1){
            		parity_command* parity_cmd1 = (parity_command* )malloc(sizeof(parity_command));
            		parity_cmd1-> type = 2;
            		parity_cmd1->parity_id = parity_id;
            		parity_cmd1->node_id = to_id;
            		parity_cmd1->cur_node_id = from_id;

            		parity_command* parity_cmd2 = (parity_command* )malloc(sizeof(parity_command));
					parity_cmd2-> type = 3;
            		parity_cmd2->parity_id = parity_id;
            		parity_cmd2->node_id = from_id;
            		parity_cmd2->cur_node_id = to_id;
            		Array_List_Insert(parity_cmds,(void* )parity_cmd1,-1);
            		Array_List_Insert(parity_cmds,(void* )parity_cmd2,-1);
            		parity_id++;            		
            	}
            }        
        }
    }

	free(parity_nodelist);
}

//采用二部图算法进行数据分块到节点的分配,先采用随机替代
void prepare_delete_repcmds(int stripe_id,cuckoo* delete_cmds){
	srand(time(0));
	delete_command* delete_cmd1 = NULL;
	delete_command* delete_cmd2 = NULL;
	for(int idx = stripe_id*erasure_k; idx < (stripe_id+1)*erasure_k; idx++){
		int select = 0;
		if(rep_num==3){
			select = rand()%3;
			delete_cmd1 = (delete_command*)malloc(sizeof(delete_command));
			delete_cmd2 = (delete_command*)malloc(sizeof(delete_command));
			delete_cmd1->type = 4;
			delete_cmd2->type = 4;
			delete_cmd1->block_id = idx;
			delete_cmd2->block_id = idx;
			if(select==0){
				delete_cmd1->node_id = placement[idx*rep_num];
				delete_cmd2->node_id = placement[idx*rep_num+1];
			} 
			if(select==1){
				delete_cmd1->node_id = placement[idx*rep_num];
				delete_cmd2->node_id = placement[idx*rep_num+2];
			}
			if(select==2){
				delete_cmd1->node_id = placement[idx*rep_num+1];
				delete_cmd2->node_id = placement[idx*rep_num+2];
			}
			
			char* key = itoa(delete_cmd1->node_id);
			if(cuckoo_exists(delete_cmds,key)){
				Array_List* cmds = (Array_List*)cuckoo_get(delete_cmds,key);
				Array_List_Insert(cmds,(void*)delete_cmd1,-1);
			}else{
				Array_List* cmds = Array_List_Init();
				Array_List_Insert(cmds,(void*)delete_cmd1,-1);
				cuckoo_insert(delete_cmds,key,(void*)cmds);
			}
			free(key);

			key = itoa(delete_cmd2->node_id);
			if(cuckoo_exists(delete_cmds,key)){
				Array_List* cmds = (Array_List*)cuckoo_get(delete_cmds,key);
				Array_List_Insert(cmds,(void*)delete_cmd2,-1);
			}else{
				Array_List* cmds = Array_List_Init();
				Array_List_Insert(cmds,(void*)delete_cmd2,-1);
				cuckoo_insert(delete_cmds,key,(void*)cmds);
			}
			free(key);
		}

		if (rep_num==2){
			select = rand()%2;
			delete_cmd1 = (delete_command*)malloc(sizeof(delete_command));
			delete_cmd1->type = 4;
			delete_cmd1->block_id = idx;
			if(select==0){
				delete_cmd1->node_id = placement[idx*rep_num];
			} 
			if(select==1){
				delete_cmd1->node_id = placement[idx*rep_num+1];
			}

			char* key = itoa(delete_cmd1->node_id);
			if(cuckoo_exists(delete_cmds,key)){
				Array_List* cmds = (Array_List*)cuckoo_get(delete_cmds,key);
				Array_List_Insert(cmds,(void*)delete_cmd1,-1);
			}else{
				Array_List* cmds = Array_List_Init();
				Array_List_Insert(cmds,(void*)delete_cmd1,-1);
				cuckoo_insert(delete_cmds,key,(void*)cmds);
			}
			free(key);
		}	
	}
}

void send_delete_cmd(int target_id,Array_List* delete_cmds){
	char* key = itoa(target_id);
	char* target_ip = cuckoo_get(node_id2IPmap,key);
	free(key);
	int conn_fd = connect_try(target_ip,dn_recv_cmd_port);
	char* ctype = itoa(4);
	send_bytes(conn_fd,ctype,sizeof(int));
	free(ctype);

	char* clen = itoa(delete_cmds->length);
	send_bytes(conn_fd,clen,sizeof(int));
	free(clen);	
	//序列化数据
	size_t needsend = sizeof(delete_command);
	char* sendbuf = (char* )malloc(sizeof(char)*needsend);
	for(int i=0;i<delete_cmds->length;i++){
		delete_command* cmd = (delete_command*)Array_List_GetAt(delete_cmds,i);
		memcpy(sendbuf,cmd,needsend);
		send_bytes(conn_fd,sendbuf,needsend);
	}
	free(sendbuf);
	//关闭网络
	close(conn_fd);
}

void wait_deletecmds_ACK(cuckoo* delete_cmds){
	char* clen = (char*)malloc(sizeof(int));
	char* buf = (char*)malloc(sizeof(delete_ack));
	delete_ack* ack = (delete_ack*)malloc(sizeof(delete_ack));
	int listen_fd = server_init(cd_recv_ack_port,10);
	int conn_fd;
	while(1){
		if(delete_cmds->size==0)
			break;
		conn_fd = server_accept(listen_fd);

		recv_bytes(conn_fd,clen,sizeof(int));
		int len = atoi(clen);

		for(int i=0;i<len;i++){
			recv_bytes(conn_fd,buf,sizeof(delete_ack));
			memcpy(ack,buf,sizeof(delete_ack));
			char* key = itoa(ack->node_id);
			Array_List* list = (Array_List*)cuckoo_get(delete_cmds,key);
			
			int idx = 0;
			for(int idx = 0; idx < list->length; idx++){
				delete_command* delete_cmd = (delete_command*)Array_List_GetAt(list,idx);
				if(delete_cmd->type == ack->type&&delete_cmd->block_id == ack->block_id&&delete_cmd->node_id==ack->node_id){
					Array_List_RemoveAt(list,idx);
	    			free(delete_cmd);
	    			break;
				}
			}

			if(list->length==0){
				cuckoo_remove(delete_cmds,key);
				Array_List_Free(list);
			}
			free(key);
		}
		close(conn_fd);
	}
	close(listen_fd);
	free(ack);
	free(buf);
	free(clen);
}

void send_parity_cmd(parity_command* parity_cmd){
	char* key = itoa(parity_cmd->cur_node_id);
	char* target_ip = cuckoo_get(node_id2IPmap,key);
	free(key);
	int conn_fd = connect_try(target_ip,dn_recv_cmd_port);
	//序列化然后发出cmd
	char* ctype = itoa(parity_cmd->type);
	send_bytes(conn_fd,ctype,sizeof(int));
	free(ctype);

	size_t needsend = sizeof(parity_command);
	char* sendbuf = (char* )malloc(sizeof(char)*needsend);
	memcpy(sendbuf,parity_cmd,needsend);
	send_bytes(conn_fd,sendbuf,needsend);
	free(sendbuf);
	//关闭网络
	close(conn_fd);	
}

void wait_paritycmds_ACK(Array_List* parity_cmds){
	char* buf = (char* )malloc(sizeof(parity_ack));
	parity_ack* ack = (parity_ack* )malloc(sizeof(parity_ack));
	int listen_fd = server_init(cd_recv_ack_port,10);
	int conn_fd;

	while(1){
		//没收到一个cmd的ack，从list中删除并且free，当list为空时，表示所有ack已经收到
		if (parity_cmds->length==0)
			break;
		conn_fd = server_accept(listen_fd);
	    recv_bytes(conn_fd,buf,sizeof(parity_ack));
	    memcpy(ack,buf,sizeof(parity_ack));
	    int idx = 0;
	    for(int idx = 0; idx < parity_cmds->length; idx++){
	    	parity_command* parity_cmd = (parity_command*)Array_List_GetAt(parity_cmds,idx);
	    	if(parity_cmd->type==ack->type&&parity_cmd->parity_id == ack->parity_id&&parity_cmd->cur_node_id==ack->cur_node_id){
	    		Array_List_RemoveAt(parity_cmds,idx);
	    		free(parity_cmd);
	    		break;
	    	}
	    }
	    close(conn_fd);
	}
	close(listen_fd);
	free(ack);
	free(buf);

}

void send_pipe_cmd(pipe_command* pipe_cmd){
	//连接网络
	//printf("%d\n",pipe_cmd->type);
	char* key = itoa(pipe_cmd->cur_node_id);
	char* target_ip = cuckoo_get(node_id2IPmap,key);
	free(key);
	int conn_fd = connect_try(target_ip,dn_recv_cmd_port);
	//序列化然后发出cmd
	char* ctype = itoa(pipe_cmd->type);
	send_bytes(conn_fd,ctype,sizeof(int));
	free(ctype);

	size_t needsend = sizeof(pipe_command);
	char* sendbuf = (char* )malloc(sizeof(char)*needsend);
	memcpy(sendbuf,pipe_cmd,needsend);
	send_bytes(conn_fd,sendbuf,needsend);
	free(sendbuf);
	//关闭网络
	close(conn_fd);
}

void wait_pipecmds_ACK(Array_List* pipe_cmds){
	//等待所有的pipe_cmd发回的ack，表示该条带的流水线归档部分已经执行完毕
	//创建接受服务器
	char* buf = (char* )malloc(sizeof(pipe_ack));
	pipe_ack* ack = (pipe_ack* )malloc(sizeof(pipe_ack));
	int opt = 1;
	int listen_fd = server_init(cd_recv_ack_port,10);
	int conn_fd;

	while(1){
		//没收到一个cmd的ack，从list中删除并且free，当list为空时，表示所有ack已经收到
		if (pipe_cmds->length==0)
			break;
		conn_fd = server_accept(listen_fd);
	    recv_bytes(conn_fd,buf,sizeof(pipe_ack));
	    memcpy(ack,buf,sizeof(pipe_ack));
	    int idx = 0;
	    for(int idx = 0;idx<pipe_cmds->length;idx++){
	    	pipe_command* pipe_cmd = (pipe_command*)Array_List_GetAt(pipe_cmds,idx);
	    	if(pipe_cmd->type==ack->type&&pipe_cmd->stripe_id==ack->stripe_id&&pipe_cmd->pipe_id==ack->pipe_id&&pipe_cmd->cur_node_id==ack->node_id){
	    		Array_List_RemoveAt(pipe_cmds,idx);
	    		free(pipe_cmd);
	    		break;
	    	}
	    }
	    close(conn_fd);
	}
	close(listen_fd);
	free(ack);
	free(buf);
}

void do_pipe_arch(archpipe_sches* sches){
		struct timeval bg_tm, ed_tm;
		double time = 0.0;
		gettimeofday(&bg_tm,NULL);
		Array_List* pipe_cmds = Array_List_Init();
		prepare_pipecmds(sches,pipe_cmds);
		for(int j = 0;j<pipe_cmds->length;j++){
			pipe_command* pipe_cmd = (pipe_command* )Array_List_GetAt(pipe_cmds,j);
			send_pipe_cmd(pipe_cmd);
		}
		printf("the pipeline_task have been sent to datanodes\n");
		wait_pipecmds_ACK(pipe_cmds);
		Array_List_Free(pipe_cmds);
		printf("the process of pipeline archival has done\n");
		gettimeofday(&ed_tm,NULL);
		time = ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000;
		encoding_time = encoding_time + time;

		gettimeofday(&bg_tm,NULL);
		Array_List* parity_cmds = Array_List_Init();		
		prepare_relocate_paritycmds(sches,parity_cmds);
		for (int j = 0; j < parity_cmds->length; ++j){
			parity_command* parity_cmd = (parity_command* )Array_List_GetAt(parity_cmds,j); 
			printf("%dth parity_cmd: type:%d,parity_id:%d,cur_node_id:%d,node_id:%d\n",j,parity_cmd->type,parity_cmd->parity_id,parity_cmd->cur_node_id,parity_cmd->node_id);
			send_parity_cmd(parity_cmd);
		}
		printf("the parity_relocation_tasks have been sent to datanodes\n");
		wait_paritycmds_ACK(parity_cmds);
		Array_List_Free(parity_cmds);
		printf("the process of parity relocations has done\n");
		gettimeofday(&ed_tm,NULL);
		time = ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000;
		parity_time = parity_time + time;

		gettimeofday(&bg_tm,NULL);
		//Array_List* delete_cmds = Array_List_Init();
		cuckoo* delete_cmds = cuckoo_init(10); 
		prepare_delete_repcmds(sches->stripe_id,delete_cmds);

		for (int j = 0; j < delete_cmds->cap; ++j){
			if(delete_cmds->nodes[j].taken){
				Array_List* cmds = (Array_List*)delete_cmds->nodes[j].val;
				char* key = delete_cmds->nodes[j].key;
				int target_id = atoi(key);
				send_delete_cmd(target_id,cmds);
			}
		}
		printf("the delete_replicas_tasks have been sent to datanodes\n");
		wait_deletecmds_ACK(delete_cmds);
		cuckoo_destroy(delete_cmds,0);
		gettimeofday(&ed_tm,NULL);
		time = ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000;
		delete_time = delete_time + time;
		printf("the process of deleting replicas has done\n");
		free_archpipe_sches(sches);
		printf("the process archival for stripe has done\n");
}

//================================================================================
//集中式归档的调度以及归档执行过程函数
archcentral_sches* init_archcentral_sches(){
	archcentral_sches* sches = (archcentral_sches*)malloc(sizeof(archcentral_sches));
	sches->central_nodeids = (int* )malloc(sizeof(int)*erasure_k);
	sches->central_blockids = (int**)malloc(sizeof(int*)*erasure_k);
	for(int i = 0;i<erasure_k;i++){
		sches->central_nodeids[i] = -1;
		sches->central_blockids[i] = (int*)malloc(sizeof(int)*erasure_k);
		for(int j = 0;j<erasure_k;j++){
			sches->central_blockids[i][j] = -1;
		}
	}
	return sches;
}

void free_archcentral_sches(archcentral_sches* sches){
	for(int i=0;i<erasure_k;i++)
		free(sches->central_blockids[i]);
	free(sches->central_blockids);
	free(sches->central_nodeids);
	free(sches);
}

encodenode_command* prepare_encodenode_cmd(archcentral_sches* sches){
	encodenode_command* encodenode_cmd = (encodenode_command*)malloc(sizeof(encodenode_command));
	encodenode_cmd->type = 5;
	encodenode_cmd->stripe_id = sches->stripe_id;
	encodenode_cmd->encode_id = sches->encode_id;
	for (int i = 0; i < erasure_k; ++i){
		encodenode_cmd->self_block_ids[i] = -1;
		encodenode_cmd->retrive_node_ids[i] = -1;
		encodenode_cmd->retrive_block_nums[i] = -1;
	}
	int count = 0;
	for(int i = 0; i<erasure_k;i++){
		if(sches->central_nodeids[i]==-1)
			continue;
		if(sches->central_nodeids[i]==sches->encode_id){
			for(int j = 0; j<erasure_k;j++)
				encodenode_cmd->self_block_ids[j] = sches->central_blockids[i][j];
		}else{
			encodenode_cmd->retrive_node_ids[count] = sches->central_nodeids[i];
			for(int j=0;j<erasure_k;j++){
				if(sches->central_blockids[i][j]==-1){
					encodenode_cmd->retrive_block_nums[count] = j;
					break;
				}
			}
			count++;
		}
	}
	return encodenode_cmd;
}

void send_encodenode_cmd(encodenode_command* encodenode_cmd){
	char* key  = itoa(encodenode_cmd->encode_id);
	char* target_ip = (char*)cuckoo_get(node_id2IPmap,key);
	free(key); 
	int conn_fd = connect_try(target_ip,dn_recv_cmd_port);
	char* ctype = itoa(encodenode_cmd->type);
	send_bytes(conn_fd,ctype,sizeof(int));
	free(ctype);

	int sendlen = sizeof(encodenode_command);
	char* sendbuf = (char*)malloc(sendlen);
	memcpy(sendbuf,encodenode_cmd,sendlen);
	send_bytes(conn_fd,sendbuf,sendlen);
	free(sendbuf);
	close(conn_fd);
}

void prepare_retrivenode_cmd(archcentral_sches* sches,Array_List* retrivenode_cmds){
	for(int i = 0; i<erasure_k; i++){
		if(sches->central_nodeids[i]!=sches->encode_id&&sches->central_nodeids[i]!=-1){
			retrivenode_command* retrivenode_cmd = (retrivenode_command*)malloc(sizeof(retrivenode_command));
			for (int j = 0; j < erasure_k; ++j)
				retrivenode_cmd->retrive_block_ids[j] = -1;
			retrivenode_cmd->type = 6;
			retrivenode_cmd->stripe_id = sches->stripe_id;
			retrivenode_cmd->encode_id = sches->encode_id;
			retrivenode_cmd->node_id = sches->central_nodeids[i];
			for(int j=0;j<erasure_k;j++)
				retrivenode_cmd->retrive_block_ids[j] = sches->central_blockids[i][j];
			Array_List_Insert(retrivenode_cmds,(void*)retrivenode_cmd,-1);
		}
	}
}

void send_retrivenode_cmd(Array_List* retrivenode_cmds){
	for(int i = 0;i<retrivenode_cmds->length;i++){
		retrivenode_command* retrivenode_cmd = (retrivenode_command*)Array_List_GetAt(retrivenode_cmds,i);
		char* key = itoa(retrivenode_cmd->node_id);
		char* target_ip = (char*)cuckoo_get(node_id2IPmap,key);
		free(key);
		int conn_fd = connect_try(target_ip,dn_recv_cmd_port);
		char* ctype = itoa(retrivenode_cmd->type);
		send_bytes(conn_fd,ctype,sizeof(int));
		free(ctype);
		int sendlen = sizeof(retrivenode_command);
		char* sendbuf = (char*)malloc(sendlen);
		memcpy(sendbuf,retrivenode_cmd,sendlen);
		send_bytes(conn_fd,sendbuf,sendlen);
		free(sendbuf);
		close(conn_fd);
		free(retrivenode_cmd);
	}
}

void wait_encodenode_ACK(encodenode_command* encodenode_cmd){
	int server_fd = server_init(cd_recv_ack_port,10);
	int conn_fd = server_accept(server_fd);
	encodenode_ack* ack = (encodenode_ack*)malloc(sizeof(encodenode_ack));
	int recvlen = sizeof(encodenode_ack);
	char* recvbuf = (char*)malloc(recvlen);
	recv_bytes(conn_fd,recvbuf,recvlen);
	memcpy(ack,recvbuf,recvlen);
	free(recvbuf);
	if(ack->type==encodenode_cmd->type&&ack->stripe_id==encodenode_cmd->stripe_id&&ack->encode_id==encodenode_cmd->encode_id){
		printf("enter the process of parity relocations\n");
	}else{
		perror("error encodenode_ack received");
	}
	free(encodenode_cmd);
	free(ack);
	close(conn_fd);
	close(server_fd);
}

void prepare_centralparity_cmd(archcentral_sches* sches,Array_List* centralparity_cmds){
	Array_List* node_list = Array_List_Init();
	for(int i=0;i<node_num;i++){
		if(sches->encode_id!=i)
			Array_List_Insert(node_list,(void*)i,-1);
	}
	parity_id = 0;
	for(int i=0;i<erasure_r-1;i++){
		int idx = -1;
		int record_bw = 0;
		int to_id = -1;	
		for(int j=0;j<node_list->length;j++){
			int nid = (int)Array_List_GetAt(node_list,j);
			double nw = network[sches->encode_id][nid];
			if(nw>record_bw){
				idx = j;
				record_bw = nw;
				to_id = nid;
			}
		}
		Array_List_RemoveAt(node_list,idx);
		parity_command* parity_cmd1 = (parity_command*)malloc(sizeof(parity_command));
		parity_cmd1->type =  2;
		parity_cmd1->parity_id = parity_id;
		parity_cmd1->node_id = to_id;
		parity_cmd1->cur_node_id = sches->encode_id;
		Array_List_Insert(centralparity_cmds,(void*)parity_cmd1,-1);
		parity_command* parity_cmd2 = (parity_command*)malloc(sizeof(parity_command));
		parity_cmd2->type =  3;
		parity_cmd2->parity_id = parity_id;
		parity_cmd2->node_id = sches->encode_id;
		parity_cmd2->cur_node_id = to_id;
		Array_List_Insert(centralparity_cmds,(void*)parity_cmd2,-1);
		parity_id++;
	}
	Array_List_Free(node_list);
}

archcentral_sches* random_schedule_central_archival(int stripe_id){
	archcentral_sches* sches = init_archcentral_sches();
	sches->stripe_id = stripe_id;
	int* res_block_ids = (int*)malloc(sizeof(int)*erasure_k);
	int* res_node_ids = (int*)malloc(sizeof(int)*erasure_k);
	for(int i = 0; i<erasure_k;i++)
		res_block_ids[i] = stripe_cons[stripe_id][i];
		//res_block_ids[i] = stripe_id*erasure_k+i;
	srand(time(0));

	for (int i = 0; i < erasure_k; ++i){
		int idx = rand()%rep_num;
		res_node_ids[i] = placement[res_block_ids[i]*rep_num+idx];
	}

	int encode_id = rand()%node_num;
	sches->encode_id = encode_id;
	for(int i=0;i<erasure_k;i++){
		//搜索是否存在该node，存在加在该nodeid的blockids中
		for(int j=0;j<erasure_k;j++){
			if(sches->central_nodeids[j]==res_node_ids[i]){
				for(int k=0;k < erasure_k;k++){
					if(sches->central_blockids[j][k]==-1){
						sches->central_blockids[j][k]=res_block_ids[i];
						break;
					}
				}
				break;
			}
			if(sches->central_nodeids[j]==-1){
				sches->central_nodeids[j]=res_node_ids[i];
				sches->central_blockids[j][0]=res_block_ids[i];
				break;
			}
		}

	}

	free(res_node_ids);
	free(res_block_ids);
	return sches;
}

int res_encode_id = -1;
int* res_central_node_ids = NULL;
double cur_central_min_bw = 0.1;
int cur_retrive_node_num = erasure_k;

void decide_optimal_central(int* res_block_ids,int* tmp_node_ids,int index){
	if(index==erasure_k){
		Array_List* node_list = Array_List_Init();
		for(int i = 0; i<node_num; i++){
			double min_bw = 1002.0;
			Array_List_Clear(node_list);
			int encode_id = i;
			//添加不同的节点到list内，encode node除外
			for(int j = 0;j < erasure_k;j++){
				int is_exist = Array_List_IsExist(node_list,(void*)tmp_node_ids[j]);
				if(is_exist==0&&tmp_node_ids[j]!=encode_id)
					Array_List_Insert(node_list,(void*)tmp_node_ids[j],-1);
			}
			//找到当前情况下最小带宽
			for(int j=0;j<node_list->length;j++){
				int from_id = (int)Array_List_GetAt(node_list,j);
				double bw  = network[from_id][encode_id];
				if(bw<min_bw)
					min_bw = bw;
			}
			//如果理论计算时间小于之前的之前的记录，替换记录
			if(node_list->length/min_bw<cur_retrive_node_num/cur_central_min_bw){
				cur_central_min_bw = min_bw;
				cur_retrive_node_num = node_list->length;
				res_encode_id = encode_id;
				for(int j = 0;j<erasure_k;j++)
					res_central_node_ids[j] = tmp_node_ids[j];
			}

		}
		Array_List_Free(node_list);
		return;
	}
	int bid = res_block_ids[index];
	for(int i=0;i<rep_num;i++){
		int nid = placement[bid*rep_num+i];
		tmp_node_ids[index] = nid;
		decide_optimal_central(res_block_ids,tmp_node_ids,index+1);
	}
}

archcentral_sches* optimal_schedule_central_archival(int stripe_id){
	archcentral_sches* sches = init_archcentral_sches();
	sches->stripe_id = stripe_id;

	int* res_block_ids = (int*)malloc(sizeof(int)*erasure_k);
	res_central_node_ids = (int*)malloc(sizeof(int)*erasure_k);
	int* tmp_node_ids = (int*)malloc(sizeof(int)*erasure_k);
	for(int i = 0; i<erasure_k;i++)
		res_block_ids[i] = stripe_cons[stripe_id][i];
		//res_block_ids[i] = stripe_id*erasure_k+i;


	res_encode_id = -1;
	cur_central_min_bw = 0.1;
	cur_retrive_node_num = erasure_k;
	decide_optimal_central(res_block_ids,tmp_node_ids,0);

	sches->encode_id = res_encode_id;
	for(int i=0;i<erasure_k;i++){
		//搜索是否存在该node，存在加在该nodeid的blockids中
		for(int j=0;j<erasure_k;j++){
			if(sches->central_nodeids[j]==res_central_node_ids[i]){
				for(int k=0;k < erasure_k;k++){
					if(sches->central_blockids[j][k]==-1){
						sches->central_blockids[j][k]=res_block_ids[i];
						break;
					}
				}
				break;
			}
			if(sches->central_nodeids[j]==-1){
				sches->central_nodeids[j]=res_central_node_ids[i];
				sches->central_blockids[j][0]=res_block_ids[i];
				break;
			}
		}
	}

	free(tmp_node_ids);
	free(res_central_node_ids);
	free(res_block_ids);
	return sches;	
}

void display_archcentral_sches(archcentral_sches* sches){
	printf("======================================\n");
	printf("stripe_id:%d\n",sches->stripe_id);
	printf("encode_id:%d\n",sches->encode_id);
	for(int i=0;i<erasure_k;i++){
		if(sches->central_nodeids[i]!=-1){
			printf("rack_id:%d,node_id:%d,block_id:",sches->central_nodeids[i]/(node_num/zone_num),sches->central_nodeids[i]);
			for(int j=0;j<erasure_k;j++){
				if(sches->central_blockids[i][j]!=-1){
					printf("%d ",sches->central_blockids[i][j]);
				}
			}
			printf("\n");
		}
	}
	printf("======================================\n");
}

archcentral_sches* schedule_central_archival(int type,int stripe_id){
	struct timeval bg_tm, ed_tm;
	gettimeofday(&bg_tm,NULL);
	archcentral_sches* sches = NULL;
	//type=1 --->random central archival
	//type=2 --->network-aware central archival
	if(type==1){
		sches = random_schedule_central_archival(stripe_id);
	}else if(type==2){
		sches = optimal_schedule_central_archival(stripe_id);
	}else{
		perror("unknow central_archival type");
	}
	gettimeofday(&ed_tm,NULL);
	double once_time = ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000;
	schedule_time = schedule_time + once_time;
	//display_archcentral_sches(sches);
	return sches;
}

void display_encodenode_command(encodenode_command* encodenode_cmd){
	printf("type:%d\n",encodenode_cmd->type);
	printf("stripe_id:%d\n",encodenode_cmd->stripe_id);
	printf("encode_id:%d\n",encodenode_cmd->encode_id);
	printf("self_block_ids:");
	for (int i = 0; i < erasure_k; ++i)
		printf("%d ",encodenode_cmd->self_block_ids[i]);
	printf("\n");

	printf("retrive_node_ids:");
	for (int i = 0; i < erasure_k; ++i)
		printf("%d ",encodenode_cmd->retrive_node_ids[i]);
	printf("\n");

	printf("retrive_block_nums:");
	for (int i = 0; i < erasure_k; ++i)
		printf("%d ",encodenode_cmd->retrive_block_nums[i]);
	printf("\n");	
}

void do_central_archival(archcentral_sches* sches){
	struct timeval bg_tm, ed_tm;
	double time = 0.0;
	gettimeofday(&bg_tm,NULL);
	//准备一个encode_task-cmd
	encodenode_command* encodenode_cmd = prepare_encodenode_cmd(sches);
	printf("finish prepare_encodenode_cmd\n");
	//display_encodenode_command(encodenode_cmd);
	//发送 encodenode_cmd
	send_encodenode_cmd(encodenode_cmd);
	printf("finish send_encodenode_cmd\n");
	//准备n个data_task-cmd
	Array_List* retrivenode_cmds = Array_List_Init();
	prepare_retrivenode_cmd(sches,retrivenode_cmds);
	printf("finish prepare_retrivenode_cmd\n");
	send_retrivenode_cmd(retrivenode_cmds);
	printf("finish send_retrivenode_cmd\n");
	Array_List_Clear(retrivenode_cmds);
	//等待来自encode_task-ack
	wait_encodenode_ACK(encodenode_cmd);
	printf("finish wait_encodenode_ACK\n");
	gettimeofday(&ed_tm,NULL);
	time = ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000;
	encoding_time = encoding_time + time;

	gettimeofday(&bg_tm,NULL);
	//发送parity-relocation-cmd
	Array_List* parity_cmds = Array_List_Init();
	prepare_centralparity_cmd(sches,parity_cmds);
	printf("finish prepare_centralparity_cmd\n");
	for (int j = 0; j < parity_cmds->length; ++j){
		parity_command* parity_cmd = (parity_command* )Array_List_GetAt(parity_cmds,j); 
		send_parity_cmd(parity_cmd);
		printf("%dth parity_cmd: type:%d,parity_id:%d,cur_node_id:%d,node_id:%d\n",j,parity_cmd->type,parity_cmd->parity_id,parity_cmd->cur_node_id,parity_cmd->node_id);
	}
	printf("finish send_parity_cmd\n");
	wait_paritycmds_ACK(parity_cmds);
	printf("finish wait_paritycmds_ACK\n");
	//等待parity-ack
	Array_List_Clear(parity_cmds);
	gettimeofday(&ed_tm,NULL);
	time = ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000;
	parity_time = parity_time + time;

	gettimeofday(&bg_tm,NULL);
	//发送删除多余的replicas-cmd
	cuckoo* delete_cmds = cuckoo_init(10); 
	prepare_delete_repcmds(sches->stripe_id,delete_cmds);
	printf("finish prepare_delete_repcmds\n");
	for (int j = 0; j < delete_cmds->cap; ++j){
		if(delete_cmds->nodes[j].taken){
			Array_List* cmds = (Array_List*)delete_cmds->nodes[j].val;
			char* key = delete_cmds->nodes[j].key;
			int target_id = atoi(key);
			send_delete_cmd(target_id,cmds);
		}
	}
	printf("finish send_delete_cmd\n");
	//等待delete-replicas-ack
	wait_deletecmds_ACK(delete_cmds);
	printf("finish wait_deletecmds_ACK\n");
	cuckoo_destroy(delete_cmds,0);
	gettimeofday(&ed_tm,NULL);
	time = ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000;
	delete_time = delete_time+time;
	free_archcentral_sches(sches);
	printf("finish do_central_archival\n");
}
//===================================================================

void init_placement2file(){
	char* filename = (char*)malloc(256);
	memset(filename,0,30);
	sprintf(filename,"k_%d-zone_%d-node_%d-stripe_%d-scheme_%d-placement",erasure_k,zone_num,node_num,stripe_num,Block_Placement_Scheme);
	FILE* pm_file = fopen(filename,"r");
	if(pm_file==NULL){
		placement = create_block_placement(Block_Placement_Scheme);
		FILE* file = fopen(filename,"a+");
		printf("创建数据分块位置信息文件\n");
		for (int i = 0; i < erasure_k*stripe_num; ++i){
			if(rep_num==3)
				fprintf(file, "%d %d %d\n", placement[i*rep_num],placement[i*rep_num+1],placement[i*rep_num+2]);
			if(rep_num==2)
				fprintf(file, "%d %d\n", placement[i*rep_num],placement[i*rep_num+1]);	
		}	
		fclose(file);
	}else{
		placement = (int* )malloc(sizeof(int)*erasure_k*stripe_num*rep_num);
		for (int i = 0; i < erasure_k*stripe_num; ++i){
			if(rep_num==3)
				fscanf(pm_file,"%d %d %d\n",&placement[i*rep_num],&placement[i*rep_num+1],&placement[i*rep_num+2]);
			if(rep_num==2)
				fscanf(pm_file,"%d %d\n",&placement[i*rep_num],&placement[i*rep_num+1]);
		}		
		fclose(pm_file);	
	}
	free(filename);
}

void init_placement(){
	placement = create_block_placement(Block_Placement_Scheme);
}

void display_stripe_cons(int** stripe_cons){
	FILE* stripes =  fopen("stripes","w");
	for(int i = 0;i<stripe_num;i++){
		for (int j = 0; j < erasure_k; ++j){
			int blockid = stripe_cons[i][j];
			fprintf(stripes,"%d ",blockid);
		}
		fprintf(stripes,"\n");
	}
	fclose(stripes);
}

void init_network(){
	//read network info,之后可以采用iperf进行实时测量
	FILE* nw_file = fopen("network","r");
	network = (double** )malloc(sizeof(double*)*node_num);
	used_times = (int**)malloc(sizeof(int*)*node_num);
	for (int i = 0; i < node_num; ++i){
		network[i] = (double* )malloc(sizeof(double)*node_num);
		used_times[i] = (int* )malloc(sizeof(int)*node_num);
	} 
	for (int i = 0; i < node_num; ++i){
		for (int j = 0; j < node_num; ++j){
			used_times[i][j] = 1;
			if (nw_file==NULL){
				int z1 = i/(node_num/zone_num);
				int z2 = j/(node_num/zone_num);
				if(i==j){
					network[i][j] = 1001;
					continue;
				}
				if(z1 == z2){
					network[i][j] = 500+rand()%501;
				}else{
					network[i][j] = rand()%201+100;
				}
			}else{
				fscanf(nw_file,"%lf\n", &network[i][j]);
			}
		}
	}
	if (nw_file!=NULL)
		fclose(nw_file);
}

void end_archival(){
	for(int i = 0;i<node_num;i++){
		char* key = itoa(i);
		char* target_ip = (char*)cuckoo_get(node_id2IPmap,key);
		free(key);
		int conn_fd = connect_try(target_ip,dn_recv_cmd_port);
		char* ctype = itoa(7);
		send_bytes(conn_fd,ctype,sizeof(int));
		free(ctype);
		close(conn_fd);
	}
	printf("finish end_archival func\n");
}

double max = 0.0;
double min = 10000000;
double avg = 0.0;
double avg_schedule_time = 0.0;
double avg_encoding_time = 0.0;
double avg_parity_time = 0.0;
double avg_delete_time = 0.0;

int main(int argc, char const *argv[])
{
	//printf("111\n");
	init_network();
	init_IPs();
	int testtime = 0;
	while(testtime<test_time){
		init_placement();
		stripe_cons = creatre_stripe_construction(Stripe_Construction_Scheme,placement);
		//display_stripe_cons(stripe_cons);
		printf("stripe_cons finish\n");
		struct timeval bg_tm, ed_tm;
		double total_time = 0.0;
		double once_time = 0.0;

		//schedule the archpipe for each stripe
		//the default stripe construction algorithm is sequenial stripe construction for each file
		for (int i = 0; i < stripe_num; ++i){
			gettimeofday(&bg_tm, NULL);
			archpipe_sches* pipe_sches =NULL;
			archcentral_sches* central_sches = NULL;
			if (Archival_Type==0){
				pipe_sches = schedule_pipe_arch(3,i,is_Force);
			}else  if(Archival_Type==1){
				pipe_sches = schedule_pipe_arch(2,i,is_Force);
			}else if(Archival_Type==2){
				pipe_sches = schedule_pipe_arch(1,i,is_Force);
			}else if(Archival_Type==3){
				central_sches = schedule_central_archival(2,i);
			}else if(Archival_Type==4){
				central_sches = schedule_central_archival(1,i);
			}else if(Archival_Type==5){
				pipe_sches = schedule_pipe_arch(4,i,is_Force);
			}else{
				perror("unknow Archival_Type");
			}

			if(Archival_Type==0||Archival_Type==1||Archival_Type==2||Archival_Type==5){
				do_pipe_arch(pipe_sches);
				//free_archpipe_sches(pipe_sches);
			}else if(Archival_Type==3||Archival_Type==4){
				do_central_archival(central_sches);
				//free_archcentral_sches(central_sches);
			}else{
				perror("unknow Archival_Type");
			}
			//free_archpipe_sches(pipe_sches);
			//free_archcentral_sches(central_sches);
			gettimeofday(&ed_tm,NULL);
			once_time = ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000;
			total_time = total_time+once_time;
		}

		double avg_total_time = total_time/stripe_num;
		avg_schedule_time = avg_schedule_time+schedule_time/stripe_num*1000;
		avg_delete_time = avg_delete_time + delete_time/stripe_num;
		avg_encoding_time = avg_encoding_time + encoding_time/stripe_num;
		avg_parity_time = avg_parity_time + parity_time/stripe_num;  
		//printf("avg_total_time:%lf\n", total_time/stripe_num);
		//printf("schedule_time:%lfms\n",schedule_time/stripe_num*1000);
		//printf("encoding_time:%lf\n",encoding_time/stripe_num);
		//printf("parity_time:%lf\n",parity_time/stripe_num);
		//printf("delete_time:%lf\n",delete_time/stripe_num);		
		testtime++;
		if (avg_total_time>max)
			max = avg_total_time;
		if (avg_total_time<min)
			min = avg_total_time;
		avg = avg+avg_total_time;
		
		free(placement);
		printf("111\n");
		free_stripe_construction(stripe_cons);
		printf("222\n");
	} 

	printf("min:%lf\n",min);
	printf("max:%lf\n",max);
	printf("avg:%lf\n",avg/test_time);
	printf("avg_schedule_time:%lfms\n",avg_schedule_time/test_time);
	printf("avg_encoding_time:%lf\n",avg_encoding_time/test_time);
	printf("avg_parity_time:%lf\n",avg_parity_time/test_time);
	printf("avg_delete_time:%lf\n",avg_delete_time/test_time);
	//printf("dpsearch_optimalpipe_time:%lf\n",dp_single_archpipe_time/stripe_num);
	//printf("prepare_time:%lf\n",prepare_time/stripe_num);
	end_archival();

	free(used_times);
	free(network);
	free(coord_IP);
	cuckoo_destroy(node_id2IPmap,1);
	return 0;
}


//修改接口 stripe_cons+placememt可自由配置
//返回条带组织和数据块位置信息，然后进行调度

//对比方案需要的函数设计实现：
//增加一个stripe-construction 算法包括：1）顺序条带组织，2）非顺序条带组织
//增加一个block-placement 生成算法包括：1）随机放置类似与hdfs，2）encoding-oriented（EAR and ERP）
//增加一个数据读取函数包括：1）内存数据读取，2）磁盘数据读取
//增加其他流水线调度以及集中式归档调度算法包括，1）随机/优化集中式归档，2）随机/优化流水线归档，其中优化流水线为网络异构感知即ours






