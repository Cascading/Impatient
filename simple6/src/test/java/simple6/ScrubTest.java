/*
 * Copyright (c) 2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
 */

package simple6;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ScrubTest
  {

  @Test
  public void
  testMain()
    throws Exception
    {
    ScrubTest tester = new ScrubTest();
    ScrubFunction scrub = new ScrubFunction();

    assertEquals( "Scrub", "foo bar", scrub.scrubText( "FoO BAR  " ) );
    }
  }