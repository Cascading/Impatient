/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package impatient;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


public class TfIdfFunction extends BaseOperation implements Function
  {

  public TfIdfFunction( Fields fieldDeclaration )
    {
    super( 5, fieldDeclaration );
    }

  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    TupleEntry argument = functionCall.getArguments();
    String doc_id = argument.getString( 0 );
    String token = argument.getString( 1 );
    int tf_count = argument.getInteger( 2 );
    int df_count = argument.getInteger( 3 );
    int n_doc = argument.getInteger( 4 );

    Tuple result = new Tuple();
    result.add( token );
    result.add( doc_id );
    result.add( getMetric( tf_count, n_doc, df_count ) );

    functionCall.getOutputCollector().add( result );
    }

  public double getMetric( int tf_count, int n_doc, int df_count )
    {
    return (double) tf_count * Math.log( (double) n_doc / ( 1.0 + df_count ) );
    }
  }
