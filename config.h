#ifndef _CONFIG_H
#define _CONFIG_H

#define rep_num 3
#define erasure_k 6
#define erasure_r 3
#define stripe_num 10
#define node_num 12
#define zone_num 6
#define file_size (4*1024) // 64mb a file a chunk size is 64/4=16mb
#define slice_num 1024
#define test_time 10

#define cd_recv_ack_port 12345
#define dn_recv_cmd_port 12346
#define dn_recv_data_port 20000
#define Max_Wait 1000
#define Block_Placement_Scheme 1 // rr 1 ,ear 2 , erp 3
#define Stripe_Construction_Scheme 1 //ssc 1 , dsc/sice  2, tea  3
#define Data_Type 1 //0-disk / 1-memory
#define Archival_Type 2 //0-opt-mpipe/1-opt-spipe/2-rand-spipe/3-opt-central/4-rand-central/5-rmpipe
#define is_Force 0 //0-DP/1-Force


#endif
