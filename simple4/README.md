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
    hadoop jar ./build/libs/simple4.jar data/rain.txt data/en.stop output/wc
    more output/wc/part-00000

gists
=====
src code
https://gist.github.com/2912769

output data
https://gist.github.com/2912789

log file
https://gist.github.com/2912779
