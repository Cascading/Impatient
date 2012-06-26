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
    hadoop jar ./build/libs/simple1.jar data/rain.txt output/rain
    more output/rain/part-00000

gists
=====
src code
https://gist.github.com/2911714

input data
https://gist.github.com/2911686

log file
https://gist.github.com/2911681