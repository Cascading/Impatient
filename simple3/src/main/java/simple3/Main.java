/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package simple3;

import java.util.Properties;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Insert;
import cascading.operation.aggregator.Count;
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
    String wcPath = args[ 1 ];

    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, Main.class );
    HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );

    // create source taps, and read from local file system when inputs are not URLs
    Tap docTap = makeTap( docPath, new TextDelimited( true, "\t" ) );

    // create sink taps, replacing output from prior runs, if needed
    Tap wcTap = makeTap( wcPath, new TextDelimited( true, "\t" ) );

    // specify a regex operation to split the "document" text lines into a token stream
    Fields token = new Fields( "token" );
    Fields text = new Fields( "text" );
    RegexSplitGenerator splitter = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
    Fields fieldDeclaration = new Fields( "doc_id", "token" );
    Pipe docPipe = new Each( "token", text, splitter, fieldDeclaration );

    // define "ScrubFunction" to clean up the token stream
    Fields doc_id = new Fields( "doc_id" );
    docPipe = new Each( docPipe, new ScrubFunction( doc_id, token, fieldDeclaration ) );

    // determine the word counts
    Pipe wcPipe = new Pipe( "wc", docPipe );
    wcPipe = new Retain( wcPipe, token );
    wcPipe = new GroupBy( wcPipe, token );
    wcPipe = new Every( wcPipe, Fields.ALL, new Count(), Fields.ALL );

    Fields count = new Fields( "count" );
    wcPipe = new GroupBy( wcPipe, count, count );

    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef().setName( "simple" );
    flowDef.addSource( docPipe, docTap );
    flowDef.addTailSink( wcPipe, wcTap );

    // write a DOT file and run the flow
    Flow simpleFlow = flowConnector.connect( flowDef );
    simpleFlow.writeDOT( "dot/simple.dot" );
    simpleFlow.complete();
    }

  public static Tap
  makeTap( String path, Scheme scheme )
    {
    return path.matches( "^[^:]+://.*" ) ? new Hfs( scheme, path ) : new Lfs( scheme, path );
    }
  }
