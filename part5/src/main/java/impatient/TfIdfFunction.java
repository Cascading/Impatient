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
