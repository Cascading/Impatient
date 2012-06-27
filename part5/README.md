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
    hadoop jar ./build/libs/impatient.jar data/rain.txt data/en.stop output/tfidf output/wc
    more output/tfidf/part-00000

gists
=====
src code
https://gist.github.com/2918989
https://gist.github.com/2918997

output data
https://gist.github.com/2918986

log file
https://gist.github.com/2918979
