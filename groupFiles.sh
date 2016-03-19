#!/bin/bash
year="1990"
for i in {0..0}
do hadoop jar filecrush-2.2.2-SNAPSHOT.jar com.m6d.filecrush.crush.Crush -input-format text -output-format text -compress gzip -max-file-blocks 1 /user/lsde06/$((year+i))/ /user/lsde06/$((year+i))small/ $((year+i))1121121212 &
done