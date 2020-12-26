#!/bin/bash
all:coordinator datanode gen_file gen_network gen_placement
coordinator:coordinator.o cuckoo.o hash.o arraylist.o common.o stripecons.o placement.o
	gcc -o coordinator coordinator.o cuckoo.o hash.o arraylist.o common.o stripecons.o placement.o
coordinator.o:coordinator.c cuckoo.h hash.h arraylist.h config.h common.h stripecons.h placement.h
	gcc -std=c99 -c coordinator.c
datanode:datanode.o galois.o cuckoo.o hash.o arraylist.o common.o
	gcc -o datanode datanode.o galois.o cuckoo.o hash.o arraylist.o common.o -lpthread
datanode.o:datanode.c galois.h cuckoo.h hash.h arraylist.h common.h config.h
	gcc -std=c99 -c datanode.c
stripecons.o:stripecons.c common.h config.h cuckoo.h hash.h arraylist.h
	gcc -std=c99 -c stripecons.c
placement.o:placement.c common.h config.h cuckoo.h hash.h arraylist.h
	gcc -std=c99 -c placement.c
common.o:common.c common.h
	gcc -std=c99 -c common.c
cuckoo.o:cuckoo.c cuckoo.h hash.h
	gcc -std=c99 -c cuckoo.c
hash.o:hash.c hash.h
	gcc -std=c99 -c hash.c 
arraylist.o:arraylist.c arraylist.h
	gcc -std=c99 -c arraylist.c
galois.o:galois.c galois.h
	gcc -std=c99 -c galois.c
gen_file:gen_file.o
	gcc -o gen_file gen_file.o
gen_file.o:gen_file.c
	gcc -std=c99 -c gen_file.c
gen_network:gen_network.o
	gcc -o gen_network gen_network.o
gen_network.o:gen_network.c config.h
	gcc -std=c99 -c gen_network.c
gen_placement:gen_placement.o
	gcc -o gen_placement gen_placement.o
gen_placement.o:gen_placement.c config.h
	gcc -std=c99 -c gen_placement.c
.PHNOY:clean
clean:
	-rm -rf *.o
	-rm -rf coordinator
	-rm -rf datanode
	-rm -rf gen_placement
	-rm -rf gen_network
	-rm -rf gen_file