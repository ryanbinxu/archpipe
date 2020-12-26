#include "placement.h"
#include "arraylist.h"
#include "cuckoo.h"


//RR 3-Replicas 
//1->zone1 
//2 3 ->zone2
void rr_block_placements(int* placement){
	int block_num = erasure_k*stripe_num;
	srand(time(0));
	int* zone = (int* )malloc(sizeof(int)*zone_num);
	int* node = (int* )malloc(sizeof(int)*(node_num/zone_num));
	for (int i = 0; i < block_num; ++i)
	{
		//randon two different zones.
		for (int j = 0; j < zone_num; ++j) zone[j]=j;
		int r = rand()%zone_num;
		int zone_id1 = zone[r];
		zone[r] = zone[zone_num-1];
		r = rand()%(zone_num-1);
		int zone_id2 = zone[r];

		//random two different nodes for zone2, and one node for zone1.
		for (int j = 0; j < node_num/zone_num; ++j) node[j] = j;
		r = rand()%(node_num/zone_num);
		int node_id2 = zone_id2*(node_num/zone_num)+node[r];
		node[r] = node[node_num/zone_num-1];
		r = rand()%(node_num/zone_num-1);
		int node_id3 = zone_id2*(node_num/zone_num)+node[r];

		r = rand()%(node_num/zone_num);
		int node_id1 = zone_id1*(node_num/zone_num)+r;

		if(rep_num==3){
			placement[i*rep_num]=node_id1;
			placement[i*rep_num+1]=node_id2;
			placement[i*rep_num+2]=node_id3;
		}

		if(rep_num==2){
			placement[i*rep_num]=node_id1;
			placement[i*rep_num+1]=node_id2;
		}
	}
	free(zone);
	free(node);

}

//THREE REPICAS rack-level = node level fault tolerance.
void max_flow(int* placement,int stripe_id,int core_rack){
	cuckoo* blacklist = cuckoo_init(10);
	Array_List* zonelist = Array_List_Init();
	Array_List* limitlist = Array_List_Init();
	char* key = NULL;
	for (int i = 0; i < zone_num; ++i){
		if(i!=core_rack)
			Array_List_Insert(zonelist,(void*)i,-1);
		key = itoa(i);
		cuckoo_insert(blacklist,key,(void*)0);
		free(key);
	}

	for(int i = 0;i<erasure_k;i++){
		int core_node = rand()%(node_num/zone_num);
		placement[(stripe_id*erasure_k+i)*rep_num] = core_node+core_rack*(node_num/zone_num);
		while(1){
			int idx = rand()%(zonelist->length);
			int secondary_rack = (int)Array_List_GetAt(zonelist,idx);
			char* core_key = itoa(core_rack);
			int core_num = (int)cuckoo_get(blacklist,core_key);
			char* second_key = itoa(secondary_rack);
			int secondary_num = (int)cuckoo_get(blacklist,second_key);
			printf("core_rack:%d,secondary_rack:%d\n",core_rack,secondary_rack);
			printf("core_num:%d,secondary_num:%d\n",core_num,secondary_num);
			int is_success = 1;
			if(core_num>=erasure_r&&secondary_num<erasure_r){
				secondary_num++;
				cuckoo_remove(blacklist,second_key);
				cuckoo_insert(blacklist,second_key,(void*)secondary_num);
			}else if(core_num<erasure_r&&secondary_num>=erasure_r){
				core_num++;
				cuckoo_remove(blacklist,core_key);
				cuckoo_insert(blacklist,core_key,(void*)core_num);
			}else if(core_num<erasure_r&&secondary_num<erasure_r){
				int i = rand()%2;
				if(i==0){
					core_num++;
					cuckoo_remove(blacklist,core_key);
					cuckoo_insert(blacklist,core_key,(void*)core_num);
				}else{
					secondary_num++;
					cuckoo_remove(blacklist,second_key);
					cuckoo_insert(blacklist,second_key,(void*)secondary_num);
				}
			}else{
				is_success = 0;
			}

			free(core_key);
			free(second_key);
			if(is_success==1){
				int id1 = rand()%(node_num/zone_num);
				placement[(stripe_id*erasure_k+i)*rep_num+1] = id1+secondary_rack*(node_num/zone_num);
				while(1){
					int id2 = rand()%(node_num/zone_num);
					if(id2!=id1){
						placement[(stripe_id*erasure_k+i)*rep_num+2] = id2+secondary_rack*(node_num/zone_num);
						break;
					}	
				}
				break;
			}else{
				int count = 0;
				for (int j = 0; j < zone_num; ++j){
					char* k = itoa(j);
					int value = cuckoo_get(blacklist,k);
					free(k);
					if(value>=erasure_r)
						count++;
				}
				printf("count:%d\n",count);
				if(count==zone_num)
				{
					perror("ERROR:can not generate blockplacment for EAR");
					exit(1);
				}
			}
		}


	}

}

//EAR 3Replicas
//max-flow matching algorithm
void ear_block_placements(int* placement){
	srand(time(0));
	int core_rack = 0;
	for(int i = 0; i<stripe_num;i++){
		max_flow(placement,i,core_rack);
		core_rack++;
		core_rack = core_rack%zone_num;
	}

}

//ERP for in-memory stores
//2Replicas P/S
void erp_block_placements(int* placement){
	srand(time(0));
	int primary_rack = 0;
	int* p2secondary = (int*)malloc(sizeof(int)*zone_num);
	int nodenumeachrack = node_num/zone_num;
	for (int i = 0; i < zone_num; ++i)
		p2secondary[i] = (i+1)%zone_num;

	for (int i = 0; i < stripe_num; ++i){
		primary_rack = primary_rack%zone_num;
		for(int j = 0; j < erasure_k; ++j){
			int primary_node = primary_rack*nodenumeachrack+rand()%nodenumeachrack;
			int secondary_rack = p2secondary[primary_rack];
			int secondary_node = secondary_rack*nodenumeachrack+rand()%nodenumeachrack;
			secondary_rack++;
			if(secondary_rack%zone_num==primary_rack){
				secondary_rack++;	
			}
			p2secondary[primary_rack] = secondary_rack%zone_num;
			placement[(i*erasure_k+j)*rep_num] = primary_node;
			placement[(i*erasure_k+j)*rep_num+1] = secondary_node;
		}
		primary_rack++;
	}
	free(p2secondary);
}

//type=1 ---> hdfs random
//type=2 ---> EAR
//type=3 ---> ERP
int* create_block_placement(int type){
	int* placement = (int*)malloc(sizeof(int)*(stripe_num*erasure_k*rep_num));
	if (type==1){
		rr_block_placements(placement);
	}else if(type==2){
		ear_block_placements(placement);
	}else if(type==3){
		erp_block_placements(placement);
	}else{
		perror("ERROR create_block_placement type");
	}
	return placement;
}

void free_block_placement(int* placement){
	free(placement);
}