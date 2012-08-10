#!/bin/sh

UCLOUD="root@14.63.225.83"
INPUT="hyunje/apat.txt"
$INPUT="youngdeok/etl_data/"
OUTPUT="youngdeok/etl_group/output"
MAPREDUCE="FrontDriver"
DELIMITER=","
COMMAND="-group 0,1"

ssh $UCLOUD hadoop fs -rmr $OUTPUT
ssh $UCLOUD hadoop jar /root/youngdeok/hadoop-example.jar org.openflamingo.hadoop.mapreduce.$MAPREDUCE -input $INPUT -output $OUTPUT -delimiter $DELIMITER $COMMAND
#ssh $UCLOUD hadoop fs -tail -f $OUTPUT/part-*