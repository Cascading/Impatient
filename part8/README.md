Scalding Examples
=================

Example logs of running these apps are in https://gist.github.com/4371896

`Word Count` with a token scrub operation, similar to Part 3:

    rm -rf output
    scald.rb --hdfs-local src/main/scala/Example3.scala --doc data/rain.txt --wc output/wc

`Word Count` with a stop word list based on HashJoin, similar to Part 4:

    rm -rf output
    scald.rb --hdfs-local src/main/scala/Example4.scala --doc data/rain.txt --stop data/en.stop --wc output/wc 


Fat Jar
-------

If you need to use any locally built JARs, move them into the local `lib` directory:

    mkdir -p lib

Then to build a "fat jar" with Gradle, use the `build.gradle` script:

    gradle clean jar
    
    rm -rf output
    hadoop jar build/libs/part8.jar Example3 --hdfs --doc data/rain.txt --wc output/wc
    
    rm -rf output
    hadoop jar build/libs/part8.jar Example4 --hdfs --doc data/rain.txt --stop data/en.stop --wc output/wc
