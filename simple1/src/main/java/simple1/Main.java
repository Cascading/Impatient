/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package simple1;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
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

    // create the source tap
    Tap inTap = new Lfs( new TextDelimited( true, "\t" ), inPath );

    // create the sink tap
    Tap outTap = new Lfs( new TextDelimited( true, "\t" ), outPath );

    // specify a pipe to connect the taps
    Pipe simplePipe = new Pipe( "simple" );

    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef().setName( "simple" );
    flowDef.addSource( simplePipe, inTap );
    flowDef.addTailSink( simplePipe, outTap );

    // run the flow
    flowConnector.connect( flowDef ).complete();
    }
  }
