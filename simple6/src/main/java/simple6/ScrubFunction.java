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


public class ScrubFunction extends BaseOperation implements Function
  {
  private final Fields docIdField;
  private final Fields tokenField;

  public ScrubFunction( Fields docIdField, Fields tokenField, Fields fieldDeclaration )
    {
    super( 2, fieldDeclaration );
    this.docIdField = docIdField;
    this.tokenField = tokenField;
    }

  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    TupleEntry argument = functionCall.getArguments();
    String doc_id = argument.getString( docIdField );
    String token = scrubText( argument.getString( tokenField ) );

    if( token.length() > 0 )
      {
      Tuple result = new Tuple();
      result.add( doc_id );
      result.add( token );
      functionCall.getOutputCollector().add( result );
      }
    }

  public String scrubText( String text )
    {
    return text.trim().toLowerCase();
    }
  }
