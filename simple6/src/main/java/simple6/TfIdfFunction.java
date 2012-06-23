/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package simple6;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


public class TfIdfFunction extends BaseOperation implements Function
  {
  private final Fields docIdField;
  private final Fields tokenField;
  private final Fields tfCountField;
  private final Fields dfCountField;
  private final Fields nDocsField;

  public TfIdfFunction( Fields docIdField, Fields tokenField, Fields tfCountField, Fields dfCountField, Fields nDocsField, Fields fieldDeclaration )
    {
    super( 5, fieldDeclaration );
    this.docIdField = docIdField;
    this.tokenField = tokenField;
    this.tfCountField = tfCountField;
    this.dfCountField = dfCountField;
    this.nDocsField = nDocsField;
    }

  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    TupleEntry argument = functionCall.getArguments();
    String doc_id = argument.getString( docIdField );
    String token = argument.getString( tokenField );
    int tf_count = argument.getInteger( tfCountField );
    int df_count = argument.getInteger( dfCountField );
    int n_doc = argument.getInteger( nDocsField );

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
