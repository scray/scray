// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scray.client.jdbc;

import static org.junit.Assert.*;

import java.net.URISyntaxException;

import org.junit.Test;

public class ScrayURLTest {

	@Test
	public void goodScrayUrlDecomposition() {

		String testurl = "scray://127.0.0.1:8080/cassandra/myKeyspace/myColumnFamily/default";

		try {
			ScrayURL surl = new ScrayURL(testurl);
			assertTrue(surl.checkSyntax());
			assertEquals(surl.getHostAndPort(), "127.0.0.1:8080");
			assertEquals(surl.getDbSystem(), "cassandra");
			assertEquals(surl.getDbId(), "myKeyspace");
			assertEquals(surl.getTableId(), "myColumnFamily");
			assertEquals(surl.getQuerySpace(), "default");
		} catch (URISyntaxException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void badScrayUrlDetection() {

		String[] testurls = {
				"spray://127.0.0.1:8080/cassandra/myKeyspace/myColumnFamily/default",
				"scray://127.0.0.1/cassandra/myKeyspace/myColumnFamily/default",
				"scray://127.0.0.1:8080/cassandra/myKeyspace/myColumnFamily" };

		try {
			for (int i = 0; i < testurls.length; i++) {
				ScrayURL surl = new ScrayURL(testurls[i]);
				assertFalse(surl.checkSyntax());
			}
		} catch (URISyntaxException e) {
			e.printStackTrace();
			fail();
		}
	}
}
