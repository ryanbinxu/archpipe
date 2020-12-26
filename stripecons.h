#ifndef _STRIPECONS_H
#define _STRIPECONS_H

#include "config.h"
#include "common.h"

void ssc_stripe_constructions(int** stripe_cons);
void dsc_sice_stripe_constructions(int** stripe_cons,int* placement);
void dsc_sice_stripe_constructions2(int** stripe_cons,int* placement);
void tea_stripe_constructions(int** stripe_cons,int* placement);
void tea_stripe_constructions2(int** stripe_cons,int* placement);
int** creatre_stripe_construction(int type,int* placement);
void free_stripe_construction(int** stripe_cons);

#endif