/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package simple6;

import java.util.Properties;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.AssertionLevel;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.Insert;
import cascading.operation.aggregator.Count;
import cascading.operation.assertion.AssertMatches;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Checkpoint;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.SumBy;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.LeftJoin;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;


public class
  Main
  {
  public static void
  main( String[] args )
    {
    String docPath = args[ 0 ];
    String stopPath = args[ 1 ];
    String tfidfPath = args[ 2 ];
    String wcPath = args[ 3 ];
    String trapPath = args[ 4 ];
    String checkPath = args[ 5 ];

    // create source taps, and read from local file system if inputs are not URLs
    Tap docTap = makeTap( docPath, new TextDelimited( true, "\t" ) );

    Fields stop = new Fields( "stop" );
    Tap stopTap = makeTap( stopPath, new TextDelimited( stop, true, "\t" ) );

    // create sink taps, replacing output from prior runs, if needed
    Tap tfidfTap = makeTap( tfidfPath, new TextDelimited( true, "\t" ) );
    Tap wcTap = makeTap( wcPath, new TextDelimited( true, "\t" ) );

    // create sink taps for the example trap and checkpoint
    Tap trapTap = makeTap( trapPath, new TextDelimited( true, "\t" ) );
    Tap checkTap = makeTap( checkPath, new TextDelimited( true, "\t" ) );

    // use a stream assertion to error check the input data
    Pipe docPipe = new Pipe( "token" );
    AssertMatches assertMatches = new AssertMatches( "doc\\d+\\s.*" );
    docPipe = new Each( docPipe, AssertionLevel.STRICT, assertMatches );

    // specify an operation within a pipe, to split text lines into a token stream
    Fields text = new Fields( "text" );
    Fields token = new Fields( "token" );
    RegexSplitGenerator splitter = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
    Fields outputSelector = new Fields( "doc_id", "token" );
    docPipe = new Each( docPipe, text, splitter, outputSelector );

    // define "ScrubFunction" to clean up the token stream
    docPipe = new Each( docPipe, new ScrubFunction() );

    // perform a left join to remove the stop words
    Pipe stopPipe = new Pipe( "stop" );
    Pipe tokenPipe = new HashJoin( docPipe, token, stopPipe, stop, new LeftJoin() );

    // discard rows which joined with stop words, i.e., non-null after left join
    tokenPipe = new Each( tokenPipe, stop, new RegexFilter( "^$" ) );
    tokenPipe = new Retain( tokenPipe, new Fields( "doc_id", "token" ) );

    // one branch to tally the token counts for term frequency (TF)
    Pipe tfPipe = new Unique( "tf", tokenPipe, Fields.ALL );
    tfPipe = new GroupBy( tfPipe, new Fields( "doc_id", "token" ) );
    Fields tf_count = new Fields( "tf_count" );
    tfPipe = new Every( tfPipe, Fields.ALL, new Count( tf_count ), Fields.ALL );
    Fields tf_token = new Fields( "tf_token" );
    tfPipe = new Rename( tfPipe, token, tf_token );

    // one branch to count the number of documents (D)
    Fields doc_id = new Fields( "doc_id" );
    Fields tally = new Fields( "tally" );
    Fields rhs_join = new Fields( "rhs_join" );
    Fields n_docs = new Fields( "n_docs" );
    Pipe dPipe = new Unique( "d", tokenPipe, doc_id );
    dPipe = new Each( dPipe, new Insert( tally, 1 ), Fields.ALL );
    dPipe = new Each( dPipe, new Insert( rhs_join, 1 ), Fields.ALL );
    dPipe = new SumBy( dPipe, rhs_join, tally, n_docs, long.class );

    // one branch to tally the token counts for document frequency (DF)
    Pipe dfPipe = new Unique( "df", tokenPipe, Fields.ALL );
    dfPipe = new GroupBy( dfPipe, token );

    Fields df_count = new Fields( "df_count" );
    Fields df_token = new Fields( "df_token" );
    Fields lhs_join = new Fields( "lhs_join" );
    dfPipe = new Every( dfPipe, Fields.ALL, new Count( df_count ), Fields.ALL );
    dfPipe = new Rename( dfPipe, token, df_token );
    dfPipe = new Each( dfPipe, new Insert( lhs_join, 1 ), Fields.ALL );

    // use a debug to observe values in the tuple stream; turn off below
    dfPipe = new Each( dfPipe, DebugLevel.VERBOSE, new Debug( true ) );

    // join to calculate TF-IDF; IDF side is smaller so it goes on RHS of CoGroup
    Pipe idfPipe = new HashJoin( dfPipe, lhs_join, dPipe, rhs_join );

    // create a checkpoint, for tracking intermediate data in DF stream
    Checkpoint idfCheck = new Checkpoint( "checkpoint", idfPipe );
    Pipe tfidfPipe = new CoGroup( tfPipe, tf_token, idfCheck, df_token );

    // calculate TF-IDF metric
    Fields fieldDeclaration = new Fields( "token", "doc_id", "tfidf" );
    tfidfPipe = new Each( tfidfPipe, new TfIdfFunction( doc_id, tf_token, tf_count, df_count, n_docs, fieldDeclaration ) );

    // keep track of the word counts, useful for QA
    Pipe wcPipe = new Pipe( "wc", tfPipe );
    wcPipe = new Retain( wcPipe, tf_token );
    wcPipe = new GroupBy( wcPipe, tf_token );
    wcPipe = new Every( wcPipe, Fields.ALL, new Count(), Fields.ALL );

    Fields count = new Fields( "count" );
    wcPipe = new GroupBy( wcPipe, count, count );

    // connect the taps and pipes into a flow
    FlowDef flowDef = FlowDef.flowDef().setName( "simple" );
    flowDef.addSource( docPipe, docTap );
    flowDef.addSource( stopPipe, stopTap );
    flowDef.addTailSink( tfidfPipe, tfidfTap );
    flowDef.addTailSink( wcPipe, wcTap );
    flowDef.addTrap( docPipe, trapTap );
    flowDef.addCheckpoint( idfCheck, checkTap );

    // set to DebugLevel.VERBOSE for trace, or DebugLevel.NONE in production
    flowDef.setDebugLevel( DebugLevel.VERBOSE );

    // set to AssertionLevel.STRICT for all assertions, or AssertionLevel.NONE in production
    flowDef.setAssertionLevel( AssertionLevel.STRICT );

    // run the Flow
    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, Main.class );

    HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );
    Flow simpleFlow = flowConnector.connect( flowDef );
    simpleFlow.writeDOT( "dot/simple.dot" );

    CascadeConnector cascadeConnector = new CascadeConnector( properties );
    Cascade cascade = cascadeConnector.connect( simpleFlow );
    cascade.complete();
    }

  public static Tap
  makeTap( String path, Scheme scheme )
    {
    return path.matches( "^[^:]+://.*" ) ? new Hfs( scheme, path ) : new Lfs( scheme, path );
    }
  }
