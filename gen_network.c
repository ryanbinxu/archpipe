#include <stdio.h>       // linux input output 
#include <stdlib.h>      //standard library
#include <malloc.h>
#include <time.h>
#include <unistd.h>
#include <stdbool.h>
#include "config.h"

bool isSameZone(int id1,int id2){
	int zone1 = id1/(node_num/zone_num);
	int zone2 = id2/(node_num/zone_num);
	if (zone1==zone2)
		return true;
	else
		return false;
}

int main(int argc, char const *argv[])
{
	double** network = (double** )malloc(sizeof(double*)*node_num);
	for (int i = 0; i < node_num; ++i)
		network[i] = (double* )malloc(sizeof(double)*node_num);

	for (int i = 0; i < node_num; ++i){
		for (int j = 0; j < node_num; ++j){
			if (i==j){
				network[i][j] = 1001;
			}else{
				if (isSameZone(i,j))
					network[i][j] = rand()%501+500;//500-1000
				else
					network[i][j] = rand()%300+1;//0-200
			}
		}
	}

	FILE* file = fopen("network","w");
	for (int i = 0; i < node_num; ++i){
		for (int j = 0; j < node_num; ++j){
			fprintf(file, "%f\n",network[i][j]);
		}
	}
	fclose(file);
	return 0;
}