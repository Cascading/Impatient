example
=======
A minimal example to show how to run a Cascading app.
Be sure to set your `HADOOP_HOME` environment variable.

build
=====
    gradle clean jar

run
===
    rm -rf output
    hadoop jar ./build/libs/simple2.jar data/rain.txt output/wc
    more output/wc/part-00000

gists
=====
src code
https://gist.github.com/2912073

output data
https://gist.github.com/2912078

log file
https://gist.github.com/2912046