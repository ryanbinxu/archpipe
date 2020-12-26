#ifndef _PLACEMENT_H
#define _PLACEMENT_H

#include "common.h"
#include "config.h"

void rr_block_placements(int* placement);
void max_flow(int* placement,int stripe_id,int core_rack);
void ear_block_placements(int* placement);
void erp_block_placements(int* placement);
int* create_block_placement(int type);
void free_block_placement(int* placement);

#endif