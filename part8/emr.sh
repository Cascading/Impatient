#!/bin/bash -ex

NAME=part8
BUCKET=temp.cascading.org/impatient
BUILD=build/libs/
DATA=data
SOURCE=rain.txt
SINK=wc

# clear previous output
s3cmd del -r s3://$BUCKET/$SINK

# load built JAR + input data
s3cmd put $BUILD/impatient.jar s3://$BUCKET/$NAME.jar
s3cmd put $DATA/$SOURCE s3://$BUCKET/$SOURCE

# launch cluster and run
elastic-mapreduce --create --name "Scalding" \
  --debug \
  --enable-debugging \
  --log-uri s3n://$BUCKET/logs \
  --jar s3n://$BUCKET/$NAME.jar \
  --arg Example3 \
  --arg "--hdfs" \
  --arg "--doc" \
  --arg s3n://$BUCKET/$SOURCE \
  --arg "--wc" \
  --arg s3n://$BUCKET/$SINK
