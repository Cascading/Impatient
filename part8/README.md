Scalding Examples
=================

Example logs of running these apps are in https://gist.github.com/4371896

`Word Count` with a token scrub operation, similar to Part 3:

    rm -rf output
    scald.rb --hdfs-local src/scala/Example3.scala --doc data/rain.txt --wc output/wc

`Word Count` with a stop word list based on HashJoin, similar to Part 4:

    rm -rf output
    scald.rb --hdfs-local src/scala/Example4.scala --doc data/rain.txt --stop data/en.stop --wc output/wc 
