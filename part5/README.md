Cascading for the Impatient, Part 5
===================================
The goal is to expand on our Word Count example in Cascading, using an **ExpressionFunction** to calculate [TF-IDF](http://en.wikipedia.org/wiki/Tf*idf). This shows how to use a **SumBy** and also a **CoGroup** to aggregate the data needed.

We'll keep building on this example to show features related to TDD and "local" mode.

More detailed background information and step-by-step documentation is provided at https://github.com/ConcurrentCore/impatient/wiki

Build Instructions
==================
To generate an IntelliJ project use:

    gradle ideaModule

To build the sample app from the command line use:

    gradle clean jar

Before running this sample app, be sure to set your `HADOOP_HOME` environment variable. Then clear the `output` directory, then to run on a desktop/laptop with Apache Hadoop in standalone mode:

    rm -rf output
    hadoop jar ./build/libs/impatient.jar data/rain.txt output/wc data/en.stop output/tfidf

To view the results:

    more output/tfidf/part-00000

An example of log captured from a successful build+run is at https://gist.github.com/3043791

For more discussion, see the [cascading-user](https://groups.google.com/forum/?fromgroups#!forum/cascading-user) email forum.

Stay tuned for the next installments of our [Cascading for the Impatient](http://www.cascading.org/category/impatient/) series.
