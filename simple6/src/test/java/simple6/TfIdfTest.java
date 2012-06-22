/*
 * Copyright (c) 2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package simple6;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TfIdfTest
  {

  @Test
  public void
  testMain()
    throws Exception
    {
    TfIdfTest tester = new TfIdfTest();
    TfIdfFunction tfidf = new TfIdfFunction();

    assertEquals( "TF-IDF", 0.446, tfidf.getMetric( 2, 5, 3 ), 0.001 );
    }
  }