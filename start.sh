#!/bin/bash

make
./deploy.sh
node_set=("192.168.0.102" "192.168.0.104" "192.168.0.108" "192.168.0.109" "192.168.0.110" "192.168.0.112" "192.168.0.113" "192.168.0.114" "192.168.0.123" "192.168.0.126" "192.168.0.128" "192.168.0.14")
for node in ${node_set[*]}
do
	ssh root@$node "cd /home/xubin/archpipe ; ./datanode ; exit"
done
./coordinator
