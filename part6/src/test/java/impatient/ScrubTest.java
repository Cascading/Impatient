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

import java.util.ArrayList;
import java.util.Iterator;

import org.junit.Test;

import cascading.CascadingTestCase;
import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleListCollector;

public class ScrubTest extends CascadingTestCase
  {
  @Test
  public void testScrub()
    {
    Fields fieldDeclaration = new Fields( "doc_id", "token" );
    Function scrub = new ScrubFunction( fieldDeclaration );
    Tuple[] arguments = new Tuple[]{
      new Tuple( "doc_1", "FoO" ),
      new Tuple( "doc_1", " BAR " ),
      new Tuple( "doc_1", "     " ) // will be scrubed
    };

    ArrayList<Tuple> expectResults = new ArrayList<Tuple>();
    expectResults.add( new Tuple( "doc_1", "foo" ) );
    expectResults.add( new Tuple( "doc_1", "bar" ) );

    TupleListCollector collector = invokeFunction( scrub, arguments, Fields.ALL );
    Iterator<Tuple> it = collector.iterator();
    ArrayList<Tuple> results = new ArrayList<Tuple>();

    while( it.hasNext() )
      results.add( it.next() );

    assertEquals( "Scrub result is not expected", expectResults, results );
    }
  }