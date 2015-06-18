/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package impatient;

import java.util.Properties;

import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;


public class Main
  {
  public static void main( String[] args )
    {
    String inPath = args[ 0 ];
    String outPath = args[ 1 ];

    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, Main.class );
    Hadoop2MR1FlowConnector flowConnector = new Hadoop2MR1FlowConnector( properties );

    // create the source tap
    Tap inTap = new Hfs( new TextDelimited( true, "\t" ), inPath );

    // create the sink tap
    Tap outTap = new Hfs( new TextDelimited( true, "\t" ), outPath );

    // specify a pipe to connect the taps
    Pipe copyPipe = new Pipe( "copy" );

    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef().addSource( copyPipe, inTap ).addTailSink( copyPipe, outTap );

    // run the flow
    flowConnector.connect( flowDef ).complete();
    }
  }
