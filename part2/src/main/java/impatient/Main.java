/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.fluid.Fluid;
import cascading.fluid.api.assembly.Assembly.AssemblyBuilder;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
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
    FlowConnector flowConnector = new Hadoop2MR1FlowConnector( properties );

    // create source and sink taps
    Tap docTap = new Hfs( new TextDelimited( true, "\t" ), docPath );
    Tap wcTap = new Hfs( new TextDelimited( true, "\t" ), wcPath );

    AssemblyBuilder.Start builder = Fluid.assembly();

    // only returns "token"
    Pipe docPipe = builder
      .startBranch( "token" )
      .each( Fluid.fields( "text" ) )
      .function(
        Fluid.function()
          // specify a regex operation to split the "document" text lines into a token stream
          .RegexSplitGenerator().fieldDeclaration( Fluid.fields( "token" ) ).patternString( "[ \\[\\]\\(\\),.]" ).end()
      )
      .outgoing( Fields.RESULTS )
      .groupBy( Fluid.fields( "token" ) )
      .every( Fields.ALL )
      .aggregator(
        Fluid.aggregator()
          .Count( Fluid.fields( "count" ) )
      )
      .outgoing( Fields.ALL )
      .completeGroupBy()
      .completeBranch();

    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef()
      .setName( "wc" )
      .addSource( "token", docTap )
      .addTailSink( docPipe, wcTap );

    // write a DOT file and run the flow
    Flow wcFlow = flowConnector.connect( flowDef );
    wcFlow.writeDOT( "dot/wc.dot" );
    wcFlow.complete();
    }
  }
