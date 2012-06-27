Cascading for the Impatient, part 1
===================================
Bokay, our lesson today is how to write a simple [Cascading 2.0](http://www.cascading.org/) app. The goal is clear and concise: create the simpliest application possible in Cascading, while following best practices. No bangs, no whistles, just good solid code.

So here's a brief Java program, about a dozen lines long. It copies lines of text from file "A" to file "B". It uses 1 *mapper* in Apache Hadoop. No *reducer* needed.

Certainly this same work could be performed in much quicker ways, such as using `cp` on Linux. However this is merely a starting point. We'll build on this example, adding new pieces of code to explore features and strengths of Cascading. We'll keep building until we have a MapReduce implementation of [TF-IDF](http://en.wikipedia.org/wiki/Tf*idf) for scoring the relative "importance" keywords in a set of documents. In other words, Text Mining 101. What you might find if you look inside [Lucene](http://lucene.apache.org/) for example. Moreover, we'll show how to use some [TDD](http://en.wikipedia.org/wiki/Test-driven_development) features of Cascading, to build robust MapReduce apps at scale.

Source
======
Download source for this example on GitHub at https://github.com/ConcurrentCore/impatient/tree/master/simple1

For quick reference, the source for this example is listed in a [gist](https://gist.github.com/2911714) on GitHub. The input data is also listed in a [gist](https://gist.github.com/2911686).

First, we create a **source tap** to specify the input data. That data happens to be formatted as tab-separated values (TSV) with a header row:

    String inPath = args[ 0 ];
    Tap inTap = new Lfs( new TextDelimited( true, "\t" ), inPath );

Next we create a **sink tap** to specify the output data, which is also TSV:

    String outPath = args[ 1 ];
    Tap outTap = new Lfs( new TextDelimited( true, "\t" ), outPath );

Then we create a **pipe** to connect the taps:

    Pipe simplePipe = new Pipe( "simple" );

Here comes the fun part. Get your toolbelt ready, because we need to do some plumbing... Connect the taps and pipes into a **flow**:

    FlowDef flowDef = FlowDef.flowDef();
    flowDef.addSource( simplePipe, inTap );
    flowDef.addTailSink( simplePipe, outTap );

The notion of a *worflow* is the essence of Cascading. Instead of thinking in terms of "map" and "reduce" steps in a MapReduce job, we prefer to think about apps. Real-world apps tend to use lots of job steps. Those are connected, and have dependencies -- typically specified by a [directed acyclic graph](http://en.wikipedia.org/wiki/Directed_acyclic_graph) (DAG). Cascading uses **flow** objects to define how a MapReduce app -- a.k.a., a DAG of MapReduce job steps -- must be connected.

Now that we have a **flow** defined, the last line of code runs it:

    flowConnector.connect( flowDef ).complete();

Place those source lines all into a `Main` method, then build a JAR file. You should be good to go.

Build
=====
The build for this example is based on using [Gradle](http://gradle.org/). The script is in `build.gradle` and to run it:

    gradle clean jar

What you should have at this point is a JAR file which is just fine to drop into your [Maven](https://maven.apache.org/) repo. Actually, we provide a community jar repository for Cascading libraries and extensions at http://conjars.org

Run
===
Before running this sample app, you'll need to have a supported release of [Apache Hadoop](http://hadoop.apache.org/) installed. Here's what was used to develop and test our example code:

    $ hadoop -version
    Hadoop 0.20.205.0

Be sure to set your `HADOOP_HOME` environment variable. Then clear the `output` directory (Apache Hadoop insists, if you're running in standalone mode) and run the app:

    rm -rf output
    hadoop jar ./build/libs/simple1.jar data/rain.txt output/rain

In other words, the file `data/raint.txt` gets copied, TSV row by TSV row. The output text is stored in the partition file `output\rain` which you can then verify:

    more output/rain/part-00000

Here's a [log file](https://gist.github.com/2911681) from our run of the sample app. If your run looks terribly different, something is probably not set up correctly. Drop us a line on the [cascading-user](https://groups.google.com/forum/?fromgroups#!forum/cascading-user) email forum. Plenty of experienced Cascading users are discussing there and eager to help. Or visit one of our user group meetings. [Coming up real soon...]

Stay tuned for the next installments of our *Cascading for the Impatient* series.
