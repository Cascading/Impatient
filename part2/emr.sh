#!/bin/bash -ex

NAME=part2
# Change this bucket name to your own.
BUCKET=temp.cascading.org/impatient
DATA_FILE=rain.txt

# Upload the JAR file to S3.
s3cmd put build/libs/impatient.jar s3://$BUCKET/$NAME.jar
# Upload the data file we want to word count.
s3cmd put data/$DATA_FILE s3://$BUCKET/$DATA_FILE

# a ruby client for elastic mapreduce.
# see: http://aws.amazon.com/developertools/2264
elastic-mapreduce --create --name "$NAME" \
  --debug \
  --enable-debugging \
  --log-uri s3n://$BUCKET/logs \
  --jar s3n://$BUCKET/$NAME.jar \
  --arg s3n://$BUCKET/$DATA_FILE \
  --arg s3n://$BUCKET/wc
