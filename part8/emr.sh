#!/bin/bash -ex

NAME=part8
BUCKET=temp.cascading.org/impatient

s3cmd put build/libs/impatient.jar s3://$BUCKET/$NAME.jar

elastic-mapreduce --create --name "Scalding" \
  --debug \
  --enable-debugging \
  --log-uri s3n://$BUCKET/logs \
  --jar s3n://$BUCKET/part8.jar \
  --arg Example3 \
  --arg "--hdfs" \
  --arg "--doc" \
  --arg s3n://$BUCKET/rain.txt \
  --arg "--wc" \
  --arg s3n://$BUCKET/out/wc
