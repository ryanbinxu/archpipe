#include <stdio.h>       // linux input output 
#include <stdlib.h>      //standard library
#include <malloc.h>
#include <time.h>
#include <unistd.h>
#include "config.h"

//the func is used to generate the info of block placements.
//the placement scheme is like that in hdfs, a zone keeps 2 blocks, and one another zone keeps 1 block.
int main(int argc, char const *argv[])
{
	int block_num = erasure_k*stripe_num;
	int* placement = (int* )malloc(sizeof(int)*block_num*rep_num);
	int* zone = (int* )malloc(sizeof(int)*zone_num);
	int* node = (int* )malloc(sizeof(int)*(node_num/zone_num)); 
	
	srand(time(0));
	for (int i = 0; i < block_num; ++i)
	{
		//randon two different zones.
		for (int j = 0; j < zone_num; ++j) zone[j]=j;
		int r = rand()%zone_num;
		int zone_id1 = zone[r];
		zone[r] = zone[zone_num-1];
		r = rand()%(zone_num-1);
		int zone_id2 = zone[r];
		//printf("%d %d\n", zone_id1,zone_id2);

		//random two different nodes for zone1, and one node for zone2.
		for (int j = 0; j < node_num/zone_num; ++j) node[j] = j;
		r = rand()%(node_num/zone_num);
		int node_id1 = zone_id1*(node_num/zone_num)+node[r];
		node[r] = node[node_num/zone_num-1];
		r = rand()%(node_num/zone_num-1);
		int node_id2 = zone_id1*(node_num/zone_num)+node[r];

		r = rand()%(node_num/zone_num);
		int node_id3 = zone_id2*(node_num/zone_num)+r;

		placement[i*rep_num]=node_id1;
		placement[i*rep_num+1]=node_id2;
		placement[i*rep_num+2]=node_id3;
		//printf("%d %d %d \n",node_id1,node_id2,node_id3);
	}

	//output to metadata file
	FILE* pm_file = fopen("placement","w");
	for (int i = 0; i < block_num; ++i)
		fprintf(pm_file, "%d %d %d\n", placement[i*rep_num],placement[i*rep_num+1],placement[i*rep_num+2]);	
	fclose(pm_file);

	free(zone);
	free(node);
	free(placement);
	return 0;
}