/*
 * Copyright (c) 2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package impatient;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import cascading.tuple.Fields;

public class ScrubTest
  {
  @Test
  public void testMain() throws Exception
    {
    ScrubTest tester = new ScrubTest();
    Fields fieldDeclaration = new Fields( "doc_id", "token" );
    ScrubFunction scrub = new ScrubFunction( fieldDeclaration );

    assertEquals( "Scrub", "foo bar", scrub.scrubText( "FoO BAR  " ) );
    }
  }