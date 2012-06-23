/*
 * Copyright (c) 2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package simple6;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import cascading.tuple.Fields;

public class ScrubTest
  {
  @Test
  public void testMain() throws Exception
    {
    ScrubTest tester = new ScrubTest();

    Fields doc_id = new Fields( "doc_id" );
    Fields token = new Fields( "token" );
    Fields fieldDeclaration = new Fields( "doc_id", "token" );
    ScrubFunction scrub = new ScrubFunction( doc_id, token, fieldDeclaration );

    assertEquals( "Scrub", "foo bar", scrub.scrubText( "FoO BAR  " ) );
    }
  }