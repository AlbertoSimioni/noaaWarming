#!/bin/bash
year="1901"
for i in {0..100}
do
    hdfs dfs -rm $((year+i))/.listing
done