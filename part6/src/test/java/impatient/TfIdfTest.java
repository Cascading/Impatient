/*
 * Copyright (c) 2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package impatient;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import cascading.tuple.Fields;

public class TfIdfTest
  {
  @Test
  public void testMain() throws Exception
    {
    TfIdfTest tester = new TfIdfTest();
    Fields fieldDeclaration = new Fields( "token", "doc_id", "tfidf" );
    TfIdfFunction tfidf = new TfIdfFunction( fieldDeclaration );

    assertEquals( "TF-IDF", 0.446, tfidf.getMetric( 2, 5, 3 ), 0.001 );
    }
  }