/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package simple1;

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
    String inPath = args[ 0 ];
    String outPath = args[ 1 ];

    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, Main.class );
    HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );

    // create source taps, and read from local file system when inputs are not URLs
    Tap inTap = makeTap( inPath, new TextDelimited( true, "\t" ) );

    // create sink taps, replacing output from prior runs, if needed
    Tap outTap = makeTap( outPath, new TextDelimited( true, "\t" ) );

    // specify a pipe to connect the taps
    Pipe simplePipe = new Pipe( "simple" );

    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef().setName( "simple" );
    flowDef.addSource( simplePipe, inTap );
    flowDef.addTailSink( simplePipe, outTap );

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
