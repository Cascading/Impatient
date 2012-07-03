Cascading for the Impatient, Part 4
===================================
The goal is to expand on our Word Count example in Cascading, this time adding a "stop words" list of tokens to nix from the stream. We'll use a **join** in Cascading to perform that at scale.

We'll keep building on this example until we have a MapReduce implementation of [TF-IDF](http://en.wikipedia.org/wiki/Tf*idf).

More detailed background information and step-by-step documentation is provided at https://github.com/ConcurrentCore/impatient/wiki

Build Instructions
==================
To generate an IntelliJ project use:

    gradle ideaModule

To build the sample app from the command line use:

    gradle clean jar

Before running this sample app, be sure to set your `HADOOP_HOME` environment variable. Then clear the `output` directory, then to run on a desktop/laptop with Apache Hadoop in standalone mode:

    rm -rf output
    hadoop jar ./build/libs/impatient.jar data/rain.txt output/wc data/en.stop

To view the results:

    more output/wc/part-00000

An example of log captured from a successful build+run is at https://gist.github.com/3043745

For more discussion, see the [cascading-user](https://groups.google.com/forum/?fromgroups#!forum/cascading-user) email forum.

Stay tuned for the next installments of our [Cascading for the Impatient](http://www.cascading.org/category/impatient/) series.
