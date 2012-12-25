Scalding Examples
=================

`Word Count` with a token scrub operation, similar to Part 3:

    rm -rf output
    scald.rb --hdfs-local src/scala/Example3.scala --doc data/rain.txt --wc output/wc

An example log of this running is in
https://gist.github.com/4371896