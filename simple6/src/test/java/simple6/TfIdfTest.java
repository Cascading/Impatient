/*
 * Copyright (c) 2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package simple6;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import cascading.tuple.Fields;

public class TfIdfTest
  {
  @Test
  public void testMain() throws Exception
    {
    TfIdfTest tester = new TfIdfTest();

    Fields doc_id = new Fields( "doc_id" );
    Fields tf_token = new Fields( "tf_token" );
    Fields tf_count = new Fields( "tf_count" );
    Fields df_count = new Fields( "df_count" );
    Fields n_docs = new Fields( "n_docs" );
    Fields fieldDeclaration = new Fields( "token", "doc_id", "tfidf" );
    TfIdfFunction tfidf = new TfIdfFunction( doc_id, tf_token, tf_count, df_count, n_docs, fieldDeclaration );

    assertEquals( "TF-IDF", 0.446, tfidf.getMetric( 2, 5, 3 ), 0.001 );
    }
  }