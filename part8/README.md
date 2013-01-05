Scalding Examples
=================
`Word Count` with a token scrub operation -- similar to "Impatient", Part 3:

    rm -rf output
    scald.rb --hdfs-local src/main/scala/Example3.scala --doc data/rain.txt --wc output/wc

`Word Count` with a stop word list based on HashJoin, similar to Part 4:

    rm -rf output
    scald.rb --hdfs-local src/main/scala/Example4.scala --doc data/rain.txt --stop data/en.stop --wc output/wc 


Fat Jar
=======
To build a _fat jar_ with Gradle, use the `build.gradle` script:

    gradle clean jar
    
    rm -rf output
    hadoop jar build/libs/impatient.jar Example3 --hdfs --doc data/rain.txt --wc output/wc
    
    rm -rf output
    hadoop jar build/libs/impatient.jar Example4 --hdfs --doc data/rain.txt --stop data/en.stop --wc output/wc

Example logs of running these apps are in https://gist.github.com/4371896

BTW, if you need to use any locally built JARs with this script, move them into the local `lib` directory.


Amazon AWS Elastic MapReduce
============================

To run this Scalding app on the Amazon AWS cloud, you'll need to have an AWS account with credentials setup locally --
for example, in your `~/.aws_cred/` directory.

Then install these two excellent AWS tools:

* [s3cmd](http://s3tools.org/s3cmd)
* [EMR Ruby client](http://aws.amazon.com/developertools/2264)

Next, edit the `emr.sh` shell script to update the `BUCKET` variable for one of your S3 buckets.

Finally, use the `emr.sh` shell script to upload your JAR and input data, 
and run the app on [Elastic MapReduce](http://aws.amazon.com/elasticmapreduce/).


Kudos
=====
Many thanks to Sujit Pal, Chris Severs, Dean Wampler, Oscar Boykinand, Hans Doktor --
for assistance with Scalding code samples, Gradle build for Scala, etc.


More Info
=========
For more discussion, see the [cascading-user](https://groups.google.com/forum/?fromgroups#!forum/cascading-user) email forum.

Stay tuned for the next installments of our [Cascading for the Impatient](http://www.cascading.org/category/impatient/) series.
