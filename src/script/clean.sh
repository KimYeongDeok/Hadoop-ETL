#!/bin/sh

UCLOUD="root@14.63.225.83"
INPUT="youngdeok/etl_data/"
OUTPUT="youngdeok/etl_clean/output"
MAPREDUCE="Sample2Driver"
DELIMITER=","
COMMAND="-clean 0,0"

ssh $UCLOUD hadoop fs -rmr $OUTPUT
ssh $UCLOUD hadoop jar /root/youngdeok/hadoop-example.jar org.openflamingo.hadoop.mapreduce.$MAPREDUCE -input $INPUT -output $OUTPUT -delimiter $DELIMITER $COMMAND
ssh $UCLOUD hadoop fs -cat $OUTPUT/part-r-00000

