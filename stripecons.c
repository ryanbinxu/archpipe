#include "stripecons.h"
#include "arraylist.h"
#include "cuckoo.h"

//按照写入顺序进行条带组织
void ssc_stripe_constructions(int** stripe_cons){
	for (int i = 0; i < stripe_num; ++i){
		for (int j = 0; j < erasure_k; ++j){
			stripe_cons[i][j] = i*erasure_k+j;
		}
	}
}

//非顺序条带组织
//采用round robin的方式选择zone/rack
void dsc_sice_stripe_constructions(int** stripe_cons,int* placement){
	//构建zone/rack->blocklist的映射
	cuckoo* zone2blocksmap = cuckoo_init(10);
	Array_List* restlist = Array_List_Init();
	int nodenumeachrack = node_num/zone_num;
	char* key = NULL;
	for (int i = 0; i < stripe_num*erasure_k; ++i){
		Array_List_Insert(restlist,(void*)i,-1);
		int zone1 = placement[i*rep_num]/nodenumeachrack;
		int zone2 = placement[(i+1)*rep_num-1]/nodenumeachrack;
		key = itoa(zone1);
		if(cuckoo_exists(zone2blocksmap,key)==1){
			Array_List* blocklist = (Array_List*)cuckoo_get(zone2blocksmap,key);
			Array_List_Insert(blocklist,(void*)i,-1);
		}else{
			Array_List* blocklist = Array_List_Init();
			Array_List_Insert(blocklist,(void*)i,-1);
			cuckoo_insert(zone2blocksmap,key,(void*)blocklist);
		}
		free(key);
		key = itoa(zone2);
		if(cuckoo_exists(zone2blocksmap,key)==1){
			Array_List* blocklist = (Array_List*)cuckoo_get(zone2blocksmap,key);
			Array_List_Insert(blocklist,(void*)i,-1);
		}else{
			Array_List* blocklist = Array_List_Init();
			Array_List_Insert(blocklist,(void*)i,-1);
			cuckoo_insert(zone2blocksmap,key,(void*)blocklist);
		}
		free(key);
	}
	//while(1) 进行sice算法，直到所有的workrack都找不到满足需求的条带
	int stripe_id = 0;//当前构造的条带数目
	int work_zone = 0;//round-robin中当前的workrack
	int is_success = 0;
	int failure_num = 0;//记录连续构造失败的次数，超过zonenum则结束循环
	Array_List* blacklist = Array_List_Init();
	Array_List* reslist = Array_List_Init();
	while(1){
		if(failure_num==zone_num)
			break;
		work_zone = work_zone%zone_num;
		is_success = 0;
		key = itoa(work_zone);
		Array_List* blocklist = (Array_List*)cuckoo_get(zone2blocksmap,key);
		free(key);
		for(int i = 0; i < blocklist->length; i++ ){
			int blockid = (int)Array_List_GetAt(blocklist,i);
			//确定一个preferRack
			int z1 = placement[blockid*rep_num]/nodenumeachrack;
			int z2 = placement[(blockid+1)*rep_num-1]/nodenumeachrack;
			if(Array_List_IsExist(blacklist,(void*)z1)==0){
				//如果不存在，加入blacklist，加入reslist.
				Array_List_Insert(blacklist,(void*)z1,-1);
				Array_List_Insert(reslist,(void*)blockid,-1);
			}else{
				//如果存在，考察另一个zoneid是否满足需求
				if(Array_List_IsExist(blacklist,(void*)z2)==0){
					Array_List_Insert(blacklist,(void*)z2,-1);
					Array_List_Insert(reslist,(void*)blockid,-1);	
				}
			}

			if(reslist->length==erasure_k){
				is_success=1;
				break;
			}
		}
		work_zone++;
		if(is_success==1){
			//删除相关数据块信息
			for (int i = 0; i < reslist->length; ++i){
				int bid = (int)Array_List_GetAt(reslist,i);
				stripe_cons[stripe_id][i] = bid;
				int zid1 = placement[bid*rep_num]/nodenumeachrack;
				int zid2 = placement[(bid+1)*rep_num-1]/nodenumeachrack;
				key = itoa(zid1);
				Array_List* list = (Array_List*)cuckoo_get(zone2blocksmap,key);
				free(key);
				Array_List_RemoveData(list,(void*)bid);
				key = itoa(zid2);
				list = (Array_List*)cuckoo_get(zone2blocksmap,key);
				free(key);
				Array_List_RemoveData(list,(void*)bid);
				Array_List_RemoveData(restlist,(void*)bid);
			}
			failure_num=0;
			stripe_id++;
		}else{
			failure_num++;
		}
		Array_List_Clear(blacklist);
		Array_List_Clear(reslist);
	}
	//对剩余的数据分块进行顺序条带组织
	while(stripe_id<stripe_num){
		for(int i=0;i<erasure_k;i++){
			int bid = (int)Array_List_GetAt(restlist,0);
			stripe_cons[stripe_id][i] = bid;
			Array_List_RemoveAt(restlist,0);
		}
		stripe_id++;
	}
	if(restlist->length>0)
		perror("ERROR sice/dsc algorithm run");
	
	//free resource
	Array_List_Free(restlist);
	Array_List_Free(blacklist);
	Array_List_Free(reslist);
	for (int i = 0; i < zone2blocksmap->cap; i++){
        if (zone2blocksmap->nodes[i].taken){    
            Array_List_Free((Array_List*)zone2blocksmap->nodes[i].val);
        }
    }
	cuckoo_destroy(zone2blocksmap,0);
}

void order_insert(Array_List* list,int blockid,int* placement){
	int idx = 0;
	int insert_nid = placement[blockid*rep_num];
	for (idx = 0; idx < list->length; ++idx){
		int bid = (int)Array_List_GetAt(list,idx);
		int nid = placement[bid*rep_num];
		if(insert_nid<nid)
			break;
	}
	Array_List_Insert(list,(void*)blockid,idx);
}

//基于ERP进行非顺序条带组织（基于内存的stores默认采用双副本）
//1  2  3  4
//-  -
//-     -
//-        -
void tea_stripe_constructions(int** stripe_cons,int* placement){
	cuckoo* zone2primarymap = cuckoo_init(10);
	int nodenumeachrack = node_num/zone_num;
	char* key = NULL;
	for (int i = 0; i < stripe_num*erasure_k; ++i){
		int primarynid = placement[i*rep_num];
		int primaryzid = primarynid/nodenumeachrack;
		key = itoa(primaryzid);
		if(cuckoo_exists(zone2primarymap,key)==0){
			Array_List* list = Array_List_Init();
			Array_List_Insert(list,(void*)i,-1);
			cuckoo_insert(zone2primarymap,key,(void*)list);
		}else{
			Array_List* list = (Array_List*)cuckoo_get(zone2primarymap,key);
			order_insert(list,i,placement);
		}
		free(key);
	}

	int stripe_id = 0;
	int failure_num = 0;
	int is_success = 0;
	int work_zone = 0;
	Array_List* blacklist = Array_List_Init();
	Array_List* reslist = Array_List_Init();
	while(1){
		if (failure_num==zone_num)
			break;
		work_zone = work_zone%zone_num;
		is_success = 0;
		key = itoa(work_zone);
		Array_List* blocklist = (Array_List*)cuckoo_get(zone2primarymap,key);
		free(key);
		for(int i=0;i<blocklist->length;i++){
			int blockid = (int)Array_List_GetAt(blocklist,i);
			int secondarynid = placement[(blockid+1)*rep_num-1];
			int secondaryzid = secondarynid/nodenumeachrack;
			if(Array_List_IsExist(blacklist,(void*)secondaryzid)==0){
				//printf("%d\n",secondaryzid);
				Array_List_Insert(blacklist,(void*)secondaryzid,-1);
				Array_List_Insert(reslist,(void*)blockid,-1);
			}

			if(reslist->length==erasure_k){
				//printf("=====\n");
				is_success = 1;
				break;
			}
		}
		printf("=====\n");
		work_zone++;
		if(is_success==1){
			for(int i = 0;i<reslist->length;i++){
				int bid = (int)Array_List_GetAt(reslist,i);
				stripe_cons[stripe_id][i] = bid;
				int zid = placement[bid*rep_num]/nodenumeachrack;
				key = itoa(zid);
				Array_List* list = (Array_List*)cuckoo_get(zone2primarymap,key);
				free(key);
				Array_List_RemoveData(list,(void*)bid);
			}
			failure_num = 0;
			stripe_id++;
		}else{
			failure_num++;
		}
		Array_List_Clear(blacklist);
		Array_List_Clear(reslist);
	}
	//剩下的数据分块处理
	//free
	Array_List* restlist = Array_List_Init();
	for (int i = 0; i < zone2primarymap->cap; i++){
        if (zone2primarymap->nodes[i].taken){    
            Array_List* list = (Array_List*)zone2primarymap->nodes[i].val;
            for (int j = 0; j < list->length; ++j){
            	int bid = (int)Array_List_GetAt(list,j);
            	Array_List_Insert(restlist,(void*)bid,-1);
            }
            Array_List_Free(list);
        }
    }

	while(stripe_id<stripe_num){
		for(int i=0;i<erasure_k;i++){
			int bid = (int)Array_List_GetAt(restlist,0);
			stripe_cons[stripe_id][i] = bid;
			Array_List_RemoveAt(restlist,0);
		}
		stripe_id++;
	}
	if(restlist->length>0)
		perror("ERROR sice/dsc algorithm run");
	//free resource
	Array_List_Free(restlist);
	Array_List_Free(blacklist);
	Array_List_Free(reslist);
	cuckoo_destroy(zone2primarymap,0);

}



//非顺序条带组织
//采用round robin的方式选择zone/rack
void dsc_sice_stripe_constructions2(int** stripe_cons,int* placement){
	//构建zone/rack->blocklist的映射
	cuckoo* zone2blocksmap = cuckoo_init(10);
	Array_List* restlist = Array_List_Init();
	int nodenumeachrack = node_num/zone_num;
	char* key = NULL;
	for (int i = 0; i < stripe_num*erasure_k; ++i){
		Array_List_Insert(restlist,(void*)i,-1);
		int zone1 = placement[i*rep_num]/nodenumeachrack;
		int zone2 = placement[(i+1)*rep_num-1]/nodenumeachrack;
		key = itoa(zone1);
		if(cuckoo_exists(zone2blocksmap,key)==1){
			Array_List* blocklist = (Array_List*)cuckoo_get(zone2blocksmap,key);
			Array_List_Insert(blocklist,(void*)i,-1);
		}else{
			Array_List* blocklist = Array_List_Init();
			Array_List_Insert(blocklist,(void*)i,-1);
			cuckoo_insert(zone2blocksmap,key,(void*)blocklist);
		}
		free(key);
		key = itoa(zone2);
		if(cuckoo_exists(zone2blocksmap,key)==1){
			Array_List* blocklist = (Array_List*)cuckoo_get(zone2blocksmap,key);
			Array_List_Insert(blocklist,(void*)i,-1);
		}else{
			Array_List* blocklist = Array_List_Init();
			Array_List_Insert(blocklist,(void*)i,-1);
			cuckoo_insert(zone2blocksmap,key,(void*)blocklist);
		}
		free(key);
	}
	//while(1) 进行sice算法，直到所有的workrack都找不到满足需求的条带
	int stripe_id = 0;//当前构造的条带数目
	int work_zone = 0;//round-robin中当前的workrack
	int is_success = 0;
	int failure_num = 0;//记录连续构造失败的次数，超过zonenum则结束循环

	//Array_List* blacklist = Array_List_Init();
	int* black_array = (int*)malloc(sizeof(int)*zone_num);
	for(int i=0;i<zone_num;i++)
		black_array[i] = 0;


	Array_List* reslist = Array_List_Init();
	while(1){
		if(failure_num==zone_num)
			break;
		work_zone = work_zone%zone_num;
		is_success = 0;
		key = itoa(work_zone);
		Array_List* blocklist = (Array_List*)cuckoo_get(zone2blocksmap,key);
		free(key);
		for(int i = 0; blocklist!=NULL&&i < blocklist->length; i++ ){
			int blockid = (int)Array_List_GetAt(blocklist,i);
			//确定一个preferRack
			int z1 = placement[blockid*rep_num]/nodenumeachrack;
			int z2 = placement[(blockid+1)*rep_num-1]/nodenumeachrack;
			if(black_array[z1]<erasure_r){
				//如果不存在，加入blacklist，加入reslist.
				black_array[z1]++;
				Array_List_Insert(reslist,(void*)blockid,-1);
			}else{
				//如果存在，考察另一个zoneid是否满足需求
				if(black_array[z2]<erasure_r){
					black_array[z2]++;
					Array_List_Insert(reslist,(void*)blockid,-1);	
				}
			}

			if(reslist->length==erasure_k){
				is_success=1;
				break;
			}
		}
		work_zone++;
		if(is_success==1){
			//删除相关数据块信息
			for (int i = 0; i < reslist->length; ++i){
				int bid = (int)Array_List_GetAt(reslist,i);
				stripe_cons[stripe_id][i] = bid;
				int zid1 = placement[bid*rep_num]/nodenumeachrack;
				int zid2 = placement[(bid+1)*rep_num-1]/nodenumeachrack;
				key = itoa(zid1);
				Array_List* list = (Array_List*)cuckoo_get(zone2blocksmap,key);
				free(key);
				Array_List_RemoveData(list,(void*)bid);
				key = itoa(zid2);
				list = (Array_List*)cuckoo_get(zone2blocksmap,key);
				free(key);
				Array_List_RemoveData(list,(void*)bid);
				Array_List_RemoveData(restlist,(void*)bid);
			}
			failure_num=0;
			stripe_id++;
		}else{
			failure_num++;
		}
		for (int i = 0; i < zone_num; ++i)
			black_array[i] = 0;
		Array_List_Clear(reslist);
	}
	//对剩余的数据分块进行顺序条带组织
	while(stripe_id<stripe_num){
		for(int i=0;i<erasure_k;i++){
			int bid = (int)Array_List_GetAt(restlist,0);
			stripe_cons[stripe_id][i] = bid;
			Array_List_RemoveAt(restlist,0);
		}
		stripe_id++;
	}
	if(restlist->length>0)
		perror("ERROR sice/dsc algorithm run");
	
	//free resource
	Array_List_Free(restlist);
	//Array_List_Free(blacklist);
	free(black_array);
	Array_List_Free(reslist);
	for (int i = 0; i < zone2blocksmap->cap; i++){
        if (zone2blocksmap->nodes[i].taken){    
            Array_List_Free((Array_List*)zone2blocksmap->nodes[i].val);
        }
    }
	cuckoo_destroy(zone2blocksmap,0);
}


void tea_stripe_constructions2(int** stripe_cons,int* placement){
	cuckoo* zone2primarymap = cuckoo_init(10);
	int nodenumeachrack = node_num/zone_num;
	char* key = NULL;
	for (int i = 0; i < stripe_num*erasure_k; ++i){
		int primarynid = placement[i*rep_num];
		int primaryzid = primarynid/nodenumeachrack;
		key = itoa(primaryzid);
		if(cuckoo_exists(zone2primarymap,key)==0){
			Array_List* list = Array_List_Init();
			Array_List_Insert(list,(void*)i,-1);
			cuckoo_insert(zone2primarymap,key,(void*)list);
		}else{
			Array_List* list = (Array_List*)cuckoo_get(zone2primarymap,key);
			order_insert(list,i,placement);
		}
		free(key);
	}

	int stripe_id = 0;
	int failure_num = 0;
	int is_success = 0;
	int work_zone = 0;
	//Array_List* blacklist = Array_List_Init();
	int* black_array = (int*)malloc(sizeof(int)*zone_num);
	for(int i = 0;i<zone_num;i++)
		black_array[i] = 0;

	Array_List* reslist = Array_List_Init();
	//printf("-1-1-1\n");
	while(1){
		if (failure_num==zone_num)
			break;
		work_zone = work_zone%zone_num;
		is_success = 0;
		key = itoa(work_zone);
		Array_List* blocklist = (Array_List*)cuckoo_get(zone2primarymap,key);
		free(key);
		//printf("000\n");

		for(int i=0; blocklist!=NULL&&i<blocklist->length; i++){
			int blockid = (int)Array_List_GetAt(blocklist,i);
			int secondarynid = placement[(blockid+1)*rep_num-1];
			int secondaryzid = secondarynid/nodenumeachrack;
			if(black_array[secondaryzid]<erasure_r){
				//printf("%d\n",secondaryzid);
				//Array_List_Insert(blacklist,(void*)secondaryzid,-1);
				black_array[secondaryzid]++;
				Array_List_Insert(reslist,(void*)blockid,-1);
			}

			if(reslist->length==erasure_k){
				//printf("=====\n");
				is_success = 1;
				break;
			}
		}
		//printf("111\n");
		work_zone++;

		if(is_success==1){
			for(int i = 0;i<reslist->length;i++){
				int bid = (int)Array_List_GetAt(reslist,i);
				stripe_cons[stripe_id][i] = bid;
				int zid = placement[bid*rep_num]/nodenumeachrack;
				key = itoa(zid);
				Array_List* list = (Array_List*)cuckoo_get(zone2primarymap,key);
				free(key);
				Array_List_RemoveData(list,(void*)bid);
			}
			failure_num = 0;
			stripe_id++;
		}else{
			failure_num++;
		}
		//printf("222\n");
		//Array_List_Clear(blacklist);
		for(int i = 0;i < zone_num;i++)
			black_array[i] = 0;
		Array_List_Clear(reslist);
		//printf("333\n");
	}
	//剩下的数据分块处理
	//free
	//printf("=====\n");
	Array_List* restlist = Array_List_Init();
	for (int i = 0; i < zone2primarymap->cap; i++){
        if (zone2primarymap->nodes[i].taken){    
            Array_List* list = (Array_List*)zone2primarymap->nodes[i].val;
            for (int j = 0; j < list->length; ++j){
            	int bid = (int)Array_List_GetAt(list,j);
            	Array_List_Insert(restlist,(void*)bid,-1);
            }
            Array_List_Free(list);
        }
    }

	while(stripe_id<stripe_num){
		for(int i=0;i<erasure_k;i++){
			int bid = (int)Array_List_GetAt(restlist,0);
			stripe_cons[stripe_id][i] = bid;
			Array_List_RemoveAt(restlist,0);
		}
		stripe_id++;
	}
	if(restlist->length>0)
		perror("ERROR sice/dsc algorithm run");
	//free resource
	Array_List_Free(restlist);
	//Array_List_Free(blacklist);
	free(black_array);
	Array_List_Free(reslist);
	cuckoo_destroy(zone2primarymap,0);

}



//type=1 ---> ssc
//type=2 ---> dsc/sice
//type=3 ---> TEA
int** creatre_stripe_construction(int type,int* placement){
	int** stripe_cons = (int**)malloc(sizeof(int*)*stripe_num);
	for (int i = 0; i < stripe_num; ++i)
		stripe_cons[i] = (int*)malloc(sizeof(int)*erasure_k);
	if(type==1)
		ssc_stripe_constructions(stripe_cons);
	else if(type==2)
		dsc_sice_stripe_constructions2(stripe_cons,placement);
	else if(type==3)
		tea_stripe_constructions2(stripe_cons,placement);
	else
		perror("error creatre_stripe_construction type");
	return stripe_cons;
}

void free_stripe_construction(int** stripe_cons){
	for (int i = 0; i < stripe_num; ++i)
		free(stripe_cons[i]);
	free(stripe_cons);
}





