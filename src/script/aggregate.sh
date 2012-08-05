#!/bin/sh

UCLOUD="root@14.63.225.83"
MAPREDUCE="clean.CleanDriver"
INPUT="youngdeok/etl_data/"
OUTPUT="youngdeok/etl_aggregate/output"

ssh $UCLOUD hadoop fs -rmr $OUTPUT
ssh $UCLOUD hadoop jar /root/youngdeok/hadoop-example.jar org.openflamingo.hadoop.mapreduce.$MAPREDUCE $INPUT $OUTPUT
ssh $UCLOUD hadoop fs -cat $OUTPUT/part-r-00000

