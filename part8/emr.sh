#!/bin/bash

elastic-mapreduce --create --name "Scalding" \
  --debug \
  --enable-debugging \
  --log-uri s3n://temp.cascading.org/impatient/logs \
  --jar s3n://temp.cascading.org/impatient/part8.jar \
  --arg Example3 \
  --arg "--hdfs" \
  --arg "--doc" \
  --arg s3n://temp.cascading.org/impatient/rain.txt \
  --arg "--wc" \
  --arg s3n://temp.cascading.org/impatient/out/wc
