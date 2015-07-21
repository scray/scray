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
	public void goodScrayUrl1Decomposition() {

		String testurl = "jdbc:scray:stateless://1.2.3.4:8080/cassandra/myKeyspace/default";

		try {
			ScrayURL surl = new ScrayURL(testurl);
			assertEquals(surl.getSubScheme(), "scray");
			assertEquals(surl.getProtocolMode(), "stateless");
			assertArrayEquals(surl.getHostAndPort(),
					new String[] { "1.2.3.4:8080", });
			assertEquals(surl.getDbSystem(), "cassandra");
			assertEquals(surl.getDbId(), "myKeyspace");
			assertEquals(surl.getQuerySpace(), "default");
		} catch (URISyntaxException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void goodScrayUrl2Decomposition() {

		String testurl = "jdbc:scray:stateless://127.0.0.1,127.0.0.2:8080/cassandra/myKeyspace/default";

		try {
			ScrayURL surl = new ScrayURL(testurl);
			assertEquals(surl.getSubScheme(), "scray");
			assertEquals(surl.getProtocolMode(), "stateless");
			assertArrayEquals(surl.getHostAndPort(), new String[] {
					"127.0.0.1:8080", "127.0.0.2:8080" });
			assertEquals(surl.getDbSystem(), "cassandra");
			assertEquals(surl.getDbId(), "myKeyspace");
			assertEquals(surl.getQuerySpace(), "default");
		} catch (URISyntaxException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void badScrayUrlDetection() {

		String[] testurls = {
				"jdbc:scray:stateless",
				"://127.0.0.1:8080/cassandra/myKeyspace/default",
				"jdpc:scray:stateless://127.0.0.1:8080/cassandra/myKeyspace/default",
				"jdbc:spray:stateless://127.0.0.1:8080/cassandra/myKeyspace/default",
				"jdbc:scray://127.0.0.1:8080/cassandra/myKeyspace/default",
				"jdbc:scray:sateless://127.0.0.1:8080/cassandra/myKeyspace/default",
				"jdbc:scray:stateless://127.0.0.1/cassandra/myKeyspace/default",
				"jdbc:scray:stateless://127.0.0.1,127.0.0.2/cassandra/myKeyspace/default",
				"jdbc:scray:stateless://:8080/cassandra/myKeyspace/default",
				"jdbc:scray:stateless://127.0.0.1:8080/cassandra/myKeyspace",
				"jdbc:scray:stateless://127.0.0.1:8080/cassandra/myKeyspace/default/something" };

		for (int i = 0; i < testurls.length; i++) {
			try {
				new ScrayURL(testurls[i]);
				fail();
			} catch (URISyntaxException e) {
				System.out.println("Successfully found faulty URL: "
						+ e.toString());
			}
		}
	}
}
