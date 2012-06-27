Cascading for the Impatient, part 2
===================================
In our [first installment of this series](../simple1/) we showed how to create the simplest possible [Cascading 2.0](http://www.cascading.org/) application. If you haven't read that yet, it's probably best to start there.

Today's lesson takes the same app and stretches it a bit further. Undoubtedy you've seen [Word Count](http://en.wikipedia.org/wiki/Word_count) before. We would feel remiss at Cascading if we did not provide a Word Count example -- it's the "Hello World" of MapReduce apps. Fortunately, that code is one of the basic steps toward developing a [TF-IDF](http://en.wikipedia.org/wiki/Tf*idf) implementation. How convenient. We'll also show how to use Cascading to generate a visualization of your MapReduce app.

Source
======
Download source for this example on GitHub at https://github.com/ConcurrentCore/impatient/tree/master/simple2

For quick reference, source code for this example is listed in a [gist](https://gist.github.com/2912073) on GitHub. The input data stays the [same as before](https://gist.github.com/2911686). Of course, now the output different -- and it's also listed in a [gist](https://gist.github.com/2912078).

The first thing different about this code, compared with the previous example, is how we define taps. In practice you'll probably prototype code on a desktop or laptop, then run in production in a cluster. For the laptop, we use an **Lfs** tap to access files in the local file system. For production, we use an **Hfs** tap to access partition files in the Hadoop distributed file system. Our code switches between those two, based on the names of files:

    public static Tap
     makeTap( String path, Scheme scheme )
      {
      return path.matches( "^[^:]+://.*" ) ? new Hfs( scheme, path ) : new Lfs( scheme, path );
      }

Note that the names of the taps have changed. Instead of `inTap` and `outTap`, we're now using `docTap` and `wcTap`. We'll be adding more taps, so it helps to have descriptive names. Makes it simpler to follow all the plumbing.

	uses a regex to split the input text lines into a token stream
	generates a DOT file, to show the Cascading flow graphically
	physical plan: 1 Mapper, 1 Reducer

...

Place those source lines all into a `Main` method, then build a JAR file. You should be good to go.

Build
=====
The build for this example is based on using [Gradle](http://gradle.org/). The script is in `build.gradle` and to run it:

    gradle clean jar

What you should have at this point is a JAR file which is just fine to drop into your [Maven](https://maven.apache.org/) repo. Actually, we provide a community jar repository for Cascading libraries and extensions at http://conjars.org

Run
===
Before running this sample app, you'll need to have a supported release of [Apache Hadoop](http://hadoop.apache.org/) installed. Here's what was used to develop and test our example code:

    $ hadoop version
    Hadoop 0.20.205.0

Be sure to set your `HADOOP_HOME` environment variable. Then clear the `output` directory (Apache Hadoop insists, if you're running in standalone mode) and run the app:

    rm -rf output
    hadoop jar ./build/libs/impatient.jar data/rain.txt output/wc

Notice how those command line arguments align with `args[]` in the source. The file `data/rain.txt` gets copied, TSV row by TSV row. Output text gets stored in the partition file `output/rain` which you can then verify:

    more output/wc/part-00000

Here's a [log file](https://gist.github.com/2912046) from our run of the sample app, part 2. If your run looks terribly different, something is probably not set up correctly. Drop us a line on the [cascading-user](https://groups.google.com/forum/?fromgroups#!forum/cascading-user) email forum. Plenty of experienced Cascading users are discussing **taps** and **pipes** and **flows** there, and eager to help. Or visit one of our user group meetings. [Coming up real soon...]

Stay tuned for the next installments of our *Cascading for the Impatient* series.
