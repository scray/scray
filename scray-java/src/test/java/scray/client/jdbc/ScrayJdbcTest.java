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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import scray.client.finagle.FinagleThriftConnection;
import scray.service.qmodel.thriftjava.ScrayTColumn;
import scray.service.qmodel.thriftjava.ScrayTColumnInfo;
import scray.service.qmodel.thriftjava.ScrayTQuery;
import scray.service.qmodel.thriftjava.ScrayTQueryInfo;
import scray.service.qmodel.thriftjava.ScrayTRow;
import scray.service.qmodel.thriftjava.ScrayTTableInfo;
import scray.service.qmodel.thriftjava.ScrayUUID;
import scray.service.qservice.thriftjava.ScrayTResultFrame;
import scray.service.qservice.thriftjava.ScrayTService;

import com.twitter.scrooge.Option;
import com.twitter.util.Future;

public class ScrayJdbcTest {

	class MockedConnection extends FinagleThriftConnection {
		ScrayTService.FutureIface mockedClient;

		public MockedConnection() {
			super("127.0.0.1:8080");
			mockedClient = mock(ScrayTService.FutureIface.class);
		}

		public ScrayTService.FutureIface getScrayTService() {
			return mockedClient;
		}
	}

	MockedConnection con = new MockedConnection();
	UUID uuid = UUID.randomUUID();
	ScrayUUID suuid = new ScrayUUID(uuid.getLeastSignificantBits(),
			uuid.getMostSignificantBits());
	ScrayTResultFrame frame;

	@Before
	public void init() {
		// stub call to query
		when(con.mockedClient.query(any(ScrayTQuery.class))).thenReturn(
				Future.value(suuid));
		// stub first call to getResults
		when(con.mockedClient.getResults(suuid))
				.thenReturn(Future.value(frame));
	}

	void createFrame(int nr) {
		ScrayTTableInfo tinfo = new ScrayTTableInfo("myDbSystem", "myDbId",
				"myTableId");
		ScrayTQueryInfo qinfo = new ScrayTQueryInfo("myQuerySpace", tinfo,
				new LinkedList<ScrayTColumnInfo>());
		List<ScrayTRow> rlist = new LinkedList<ScrayTRow>();
		for (int j = 1; j <= 10; j++) {
			List<ScrayTColumn> clist = new LinkedList<ScrayTColumn>();
			for (int i = 1; i <= 10; i++) {
				clist.add(createColumn(i));
			}
			rlist.add(new ScrayTRow(Option.<ByteBuffer> none(), Option.make(
					true, clist)));
		}
		frame = new ScrayTResultFrame(qinfo, rlist);
	}

	ScrayTColumn createColumn(int col) {
		// ScrayTColumn tcol = new ScrayTColumn(new ScrayTColumnInfo("col" +
		// col, ));
		return null;
	}

	@Test
	public void goodScrayUrlDecomposition() {

	}

}
