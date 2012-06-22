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
import cascading.operation.Insert;
import cascading.operation.aggregator.Count;
import cascading.operation.assertion.AssertMatches;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitGenerator;
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

    // create a Scheme and a Source Tap to read the input
    Tap docTap = makeTap( docPath, new TextDelimited( true, "\t" ) );

    // create a Scheme and a Source Tap to read the stop words
    Tap stopTap = makeTap( stopPath, new TextDelimited( new Fields( "stop" ), true, "\t" ) );

    // Scheme and a Sink Tap for TF-IDF metrics
    Tap tfidfTap = makeTap( tfidfPath, new TextDelimited( true, "\t" ) );

    // Scheme and a Sink Tap for word counts, which are useful for QA
    Tap wcTap = makeTap( wcPath, new TextDelimited( true, "\t" ) );

    // Scheme and a Sink Tap for trapped input
    Tap trapTap = makeTap( trapPath, new TextDelimited( true, "\t" ) );

    // Assertions on the input data
    Pipe docPipe = new Pipe( "token" );
    AssertMatches assertMatches = new AssertMatches( "doc\\d+\\s.*" );
    docPipe = new Each( docPipe, AssertionLevel.STRICT, assertMatches );

    // specify an Operation within a Pipe to split text lines into a token stream
    RegexSplitGenerator splitter = new RegexSplitGenerator( new Fields( "token" ), "[ \\[\\]\\(\\),.]" );
    Fields outputSelector = new Fields( "doc_id", "token" );
    docPipe = new Each( docPipe, new Fields( "text" ), splitter, outputSelector );

    // define "ScrubFunction" to clean up the token stream
    docPipe = new Each( docPipe, new ScrubFunction() );

    // perform a LEFT JOIN to remove the stop words
    Pipe stopPipe = new Pipe( "stop" );
    Pipe tokenPipe = new HashJoin( docPipe, new Fields( "token" ), stopPipe, new Fields( "stop" ), new LeftJoin() );

    // discard rows which joined with stop words, i.e., non-NULL after LEFT JOIN
    tokenPipe = new Each( tokenPipe, new Fields( "stop" ), new RegexFilter( "^$" ) );
    tokenPipe = new Retain( tokenPipe, new Fields( "doc_id", "token" ) );

    // one branch to tally the token counts for term frequency (TF)
    Pipe tfPipe = new Unique( "tf", tokenPipe, Fields.ALL );
    tfPipe = new GroupBy( tfPipe, new Fields( "doc_id", "token" ) );
    tfPipe = new Every( tfPipe, Fields.ALL, new Count( new Fields( "tf_count" ) ), Fields.ALL );
    tfPipe = new Rename( tfPipe, new Fields( "token" ), new Fields( "tf_token" ) );

    // one branch to count the number of documents (D)
    Pipe dPipe = new Unique( "d", tokenPipe, new Fields( "doc_id" ) );
    dPipe = new Each( dPipe, new Insert( new Fields( "tally" ), 1 ), Fields.ALL );
    dPipe = new Each( dPipe, new Insert( new Fields( "rhs_join" ), 1 ), Fields.ALL );
    dPipe = new SumBy( dPipe, new Fields( "rhs_join" ), new Fields( "tally" ), new Fields( "n_doc" ), long.class );

    // one branch to tally the token counts for document frequency (DF)
    Pipe dfPipe = new Unique( "df", tokenPipe, Fields.ALL );
    dfPipe = new GroupBy( dfPipe, new Fields( "token" ) );
    dfPipe = new Every( dfPipe, Fields.ALL, new Count( new Fields( "df_count" ) ), Fields.ALL );
    dfPipe = new Rename( dfPipe, new Fields( "token" ), new Fields( "df_token" ) );
    dfPipe = new Each( dfPipe, new Insert( new Fields( "lhs_join" ), 1 ), Fields.ALL );

    /*
import cascading.pipe.Checkpoint;
    String checkPath = args[ 5 ];
    // Scheme and a Sink Tap for a checkpoint of intermediate data results
    Tap checkTap = makeTap( checkPath, new TextDelimited( true, "\t" ) );

    // ALSO: create a checkpoint, for testing intermediate data results
    Checkpoint dfCheckpoint = new Checkpoint( "checkpoint", idfPipe );
    //flowDef.addCheckpoint( dfCheckpoint, checkTap );
    */

    // join to calculate the TF-IDF metric
    Pipe idfPipe = new HashJoin( dfPipe, new Fields( "lhs_join" ), dPipe, new Fields( "rhs_join" ) );
    Pipe tfidfPipe = new CoGroup( tfPipe, new Fields( "tf_token" ), idfPipe, new Fields( "df_token" ) );
    tfidfPipe = new Each( tfidfPipe, new TfIdfFunction() );

    // keep track of the word counts, useful for QA
    Pipe wcPipe = new Pipe( "wc", tfPipe );
    wcPipe = new Retain( wcPipe, new Fields( "tf_token" ) );
    wcPipe = new GroupBy( wcPipe, new Fields( "tf_token" ) );
    wcPipe = new Every( wcPipe, Fields.ALL, new Count(), Fields.ALL );

    Fields sortFields = new Fields( "count" );
    wcPipe = new GroupBy( wcPipe, new Fields( "count" ), sortFields );

    // connect the Taps and Pipe into a Flow
    FlowDef flowDef = FlowDef.flowDef();
    flowDef.addSource( docPipe, docTap );
    flowDef.addSource( stopPipe, stopTap );
    flowDef.addTailSink( tfidfPipe, tfidfTap );
    flowDef.addTailSink( wcPipe, wcTap );
    flowDef.addTrap( docPipe, trapTap );

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
