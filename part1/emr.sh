#!/bin/bash -ex

NAME=part1
BUCKET=temp.cascading.org/impatient

s3cmd put build/libs/impatient.jar s3://$BUCKET/$NAME.jar

elastic-mapreduce --create --name "$NAME" \
  --debug \
  --enable-debugging \
  --log-uri s3n://$BUCKET/logs \
  --jar s3n://$BUCKET/$NAME.jar \
  --arg s3n://$BUCKET/rain.txt \
  --arg s3n://$BUCKET/rain
