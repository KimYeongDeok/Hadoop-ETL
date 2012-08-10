#!/bin/sh

UCLOUD="root@14.63.225.83"
INPUT="youngdeok/etl_data_small/"
OUTPUT="youngdeok/etl_generate/output"
MAPREDUCE="FrontDriver"
DELIMITER=","
COMMAND="-generate key"

ssh $UCLOUD hadoop fs -rmr $OUTPUT
ssh $UCLOUD hadoop jar /root/youngdeok/hadoop-example.jar org.openflamingo.hadoop.mapreduce.$MAPREDUCE -input $INPUT -output $OUTPUT -delimiter $DELIMITER $COMMAND
#ssh $UCLOUD hadoop fs -cat $OUTPUT/part-*
