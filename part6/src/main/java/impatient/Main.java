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
import cascading.operation.AssertionLevel;
import cascading.operation.DebugLevel;
import cascading.pipe.Checkpoint;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.LeftJoin;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import static cascading.fluid.Fluid.*;

public class
  Main
  {
  public static void
  main( String[] args )
    {
    String docPath = args[ 0 ];
    String wcPath = args[ 1 ];
    String stopPath = args[ 2 ];
    String tfidfPath = args[ 3 ];
    String trapPath = args[ 4 ];
    String checkPath = args[ 5 ];

    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, Main.class );
    FlowConnector flowConnector = new Hadoop2MR1FlowConnector( properties );

    // create source and sink taps
    Tap docTap = new Hfs( new TextDelimited( true, "\t" ), docPath );
    Tap wcTap = new Hfs( new TextDelimited( true, "\t" ), wcPath );

    Fields stop = new Fields( "stop" );
    Tap stopTap = new Hfs( new TextDelimited( stop, true, "\t" ), stopPath );
    Tap tfidfTap = new Hfs( new TextDelimited( true, "\t" ), tfidfPath );

    Tap trapTap = new Hfs( new TextDelimited( true, "\t" ), trapPath );
    Tap checkTap = new Hfs( new TextDelimited( true, "\t" ), checkPath );

    AssemblyBuilder.Start builder = Fluid.assembly();

    Pipe docPipe = builder
      .startBranch( "token" )
      .each( Fields.ALL )
      .assertionLevel( AssertionLevel.STRICT )
      .assertion(
        valueAssertion()
          .AssertMatches().patternString( "doc\\d+\\s.*" ).end()
      )
      .each( fields( "text" ) )
      .function(
        function()
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

    Pipe tokenPipe = builder
      .continueBranch( hashJoin )
      .each( fields( "stop" ) )
      .filter(
        filter().RegexFilter().patternString( "^$" ).end()
      )
      .retain( fields( "doc_id", "token" ) )
      .completeBranch();

    // one branch of the flow tallies the token counts for term frequency (TF)
    Pipe tfPipe = builder
      .continueBranch( "TF", tokenPipe )
      .countBy()
      .groupingFields( fields( "doc_id", "token" ) ).countField( fields( "tf_count" ) ).end()
      .rename( fields( "token" ), fields( "tf_token" ) )
      .completeBranch();

    // one branch counts the number of documents (D)
    Pipe dPipe = builder
      .continueBranch( "D", tokenPipe )
      .unique().uniqueFields( fields( "doc_id" ) ).end()
      .each( Fields.NONE )
      .function(
        function().Insert( fields( "tally" ), 1 )
      )
      .outgoing( Fields.ALL )
      .each( Fields.NONE )
      .function(
        function().Insert( fields( "rhs_join" ), 1 )
      )
      .outgoing( Fields.ALL )
      .sumBy().groupingFields( fields( "rhs_join" ) ).valueField( fields( "tally" ) ).sumField( fields( "n_docs" ) ).sumType( long.class ).end()
      .completeBranch();

    Pipe dfPipe = builder
      .continueBranch( "DF", tokenPipe )
      .unique().uniqueFields( Fields.ALL ).end()
      .countBy().groupingFields( fields( "token" ) ).countField( fields( "df_count" ) ).end()
      .rename( fields( "token" ), fields( "df_token" ) )
      .each( Fields.NONE )
      .function(
        function().Insert( fields( "lhs_join" ), 1 )
      )
      .outgoing( Fields.ALL )
      .each( Fields.ALL )
      .debugLevel( DebugLevel.VERBOSE )
      .debug(
        filter().Debug().printFields( true ).end() // example use of a debug, to observe tuple stream; turn off below
      )
      .completeBranch();

    // join to bring together all the components for calculating TF-IDF
    // the D side of the join is smaller, so it goes on the RHS
    Pipe idfPipe = builder
      .startHashJoin()
      .lhs( dfPipe ).lhsJoinFields( fields( "lhs_join" ) )
      .rhs( dPipe ).rhsJoinFields( fields( "rhs_join" ) )
      .createHashJoin();

    Checkpoint idfCheck = (Checkpoint) assembly()
      .continueBranch( idfPipe )
      .checkpoint( "checkpoint" )
      .completeBranch();

    // the IDF side of the join is smaller, so it goes on the RHS
    Pipe tfidfPipe = builder
      .startCoGroup()
      .lhs( tfPipe ).lhsGroupFields( fields( "tf_token" ) )
      .rhs( idfCheck ).rhsGroupFields( fields( "df_token" ) )
      .createCoGroup();

    // calculate the TF-IDF weights, per token, per document
    tfidfPipe = builder
      .continueBranch( tfidfPipe )
      .each( fields( "tf_count", "df_count", "n_docs" ) )
      .function(
        function()
          .ExpressionFunction()
          .fieldDeclaration( fields( "tfidf" ) )
          .expression( "(double) tf_count * Math.log( (double) n_docs / ( 1.0 + df_count ) )" )
          .parameterType( Double.class )
          .end()
      )
      .outgoing( Fields.ALL )
      .retain( fields( "tf_token", "doc_id", "tfidf" ) )
      .rename( fields( "tf_token" ), fields( "token" ) )
      .completeBranch();

    // keep track of the word counts, which are useful for QA
    Pipe wcPipe = builder
      .continueBranch( "wc", tfPipe )
      .sumBy().groupingFields( fields( "tf_token" ) ).valueField( fields( "tf_count" ) ).sumField( fields( "count" ) ).sumType( long.class ).end()
      .rename( fields( "tf_token" ), fields( "token" ) )
      .groupBy( fields( "count" ), fields( "count" ) ) // additionally, sort by count
      .completeGroupBy()
      .completeBranch();

    // connect the taps, pipes, traps, checkpoints, etc., into a flow
    FlowDef flowDef = FlowDef.flowDef()
      .setName( "tfidf" )
      .addSource( docPipe, docTap )
      .addSource( stopPipe, stopTap )
      .addTailSink( tfidfPipe, tfidfTap )
      .addTailSink( wcPipe, wcTap )
      .addTrap( docPipe, trapTap )
      .addCheckpoint( idfCheck, checkTap );

    // set to DebugLevel.VERBOSE for trace, or DebugLevel.NONE in production
    flowDef.setDebugLevel( DebugLevel.VERBOSE );

    // set to AssertionLevel.STRICT for all assertions, or AssertionLevel.NONE in production
    flowDef.setAssertionLevel( AssertionLevel.STRICT );

    // write a DOT file and run the flow
    Flow tfidfFlow = flowConnector.connect( flowDef );
    tfidfFlow.writeDOT( "dot/tfidf.dot" );
    tfidfFlow.complete();
    }
  }
