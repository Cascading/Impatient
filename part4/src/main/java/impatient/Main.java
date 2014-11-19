/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.LeftJoin;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import static cascading.fluid.Fluid.fields;

public class
  Main
  {
  public static void
  main( String[] args )
    {
    String docPath = args[ 0 ];
    String wcPath = args[ 1 ];
    String stopPath = args[ 2 ];

    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, Main.class );
    FlowConnector flowConnector = new Hadoop2MR1FlowConnector( properties );

    // create source and sink taps
    Tap docTap = new Hfs( new TextDelimited( true, "\t" ), docPath );
    Tap wcTap = new Hfs( new TextDelimited( true, "\t" ), wcPath );

    Tap stopTap = new Hfs( new TextDelimited( fields( "stop" ), true, "\t" ), stopPath );

    AssemblyBuilder.Start builder = Fluid.assembly();

    Pipe docPipe = builder
      .startBranch( "token" )
      .each( fields( "text" ) )
      .function(
        Fluid.function()
          // specify a regex operation to split the "document" text lines into a token stream
          .RegexSplitGenerator().fieldDeclaration( fields( "token" ) ).patternString( "[ \\[\\]\\(\\),.]" ).end()
      )
      .outgoing( fields( "doc_id", "token" ) )
      .each( fields( "doc_id", "token" ) ).function(
        new ScrubFunction( Fields.ARGS ) // define "ScrubFunction" to clean up the token stream
      )
      .outgoing( Fields.RESULTS )
      .completeBranch();

    Pipe stopPipe = builder
      .startBranch( "stop" )
      .completeBranch();

    // perform a left join to remove stop words, discarding the rows
    // which joined with stop words, i.e., were non-null after left join
    HashJoin hashJoin = builder.startHashJoin()
      .lhs( docPipe ).lhsJoinFields( fields( "token" ) )
      .rhs( stopPipe ).rhsJoinFields( fields( "stop" ) )
      .joiner( new LeftJoin() )
      .createHashJoin();

    Pipe wcPipe = builder
      .continueBranch( hashJoin )
      .each( fields( "stop" ) )
      .filter(
        Fluid.filter().RegexFilter().patternString( "^$" ).end()
      )
      .retain( fields( "token" ) )
      .groupBy( fields( "token" ) ) // determine the word counts
      .every( Fields.ALL )
      .aggregator(
        Fluid.aggregator()
          .Count( fields( "count" ) )
      )
      .outgoing( Fields.ALL )
      .completeGroupBy()
      .completeBranch();

    // connect the taps, pipes, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef()
      .setName( "wc" )
      .addSource( docPipe, docTap )
      .addSource( stopPipe, stopTap )
      .addTailSink( wcPipe, wcTap );

    // write a DOT file and run the flow
    Flow wcFlow = flowConnector.connect( flowDef );
    wcFlow.writeDOT( "dot/wc.dot" );
    wcFlow.complete();
    }
  }
