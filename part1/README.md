Cascading for the Impatient, Part 1
===================================
The goal is to create the simplest [Cascading 2.0](http://www.cascading.org/) app possible, while following best practices.

Here's a brief Java program which copies lines of text from file "A" to file "B". We'll keep building on this example until we have a MapReduce implementation of [TF-IDF](http://en.wikipedia.org/wiki/Tf*idf).

More detailed background information and step-by-step documentation is provided at https://github.com/ConcurrentCore/impatient/wiki

Pre-reqs
==================
You will need to have [Apache Hadoop](http://hadoop.apache.org/) installed. If you're on a Mac and have [Homebrew](http://mxcl.github.com/homebrew/), simply run the following:

    brew install hadoop

Once Hadoop has been installed , be sure to set your `HADOOP_HOME` environment variable set.  In a 'nix environment, simply run the following:

    export HADOOP_HOME=$(which hadoop)

Build Instructions
==================

To generate an IntelliJ project use:

    gradle ideaModule

To build the sample app from the command line use:

    gradle clean jar

To run on a desktop/laptop with Hadoop in standalone mode:

    hadoop jar ./build/libs/impatient.jar data/rain.txt output/rain

To view the results:

    more output/rain/part-00000

An example of log captured from a successful build+run is at https://gist.github.com/2911686

For more discussion, see the [cascading-user](https://groups.google.com/forum/?fromgroups#!forum/cascading-user) email forum.

Stay tuned for the next installments of our [Cascading for the Impatient](http://www.cascading.org/category/impatient/) series.
