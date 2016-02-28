#!/bin/bash
year="1905"
for i in {0..2}
do
unzip $((year+i)).zip
hdfs dfs -copyFromLocal $((year+i)) /user/lsde06/$((year+i))
rm $((year+i)).zip
rm -r $((year+i))
echo "Done year "
echo $((year+i))
echo "\n"
done