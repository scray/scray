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
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scray.client.finagle.ScrayStatelessTServiceAdapter;
import scray.common.serialization.pool.KryoJavaPoolSerialization;
import scray.service.qmodel.thriftjava.ScrayTColumn;
import scray.service.qmodel.thriftjava.ScrayTColumnInfo;
import scray.service.qmodel.thriftjava.ScrayTQuery;
import scray.service.qmodel.thriftjava.ScrayTQueryInfo;
import scray.service.qmodel.thriftjava.ScrayTRow;
import scray.service.qmodel.thriftjava.ScrayTTableInfo;
import scray.service.qmodel.thriftjava.ScrayUUID;
import scray.service.qservice.thriftjava.ScrayStatelessTService;
import scray.service.qservice.thriftjava.ScrayTResultFrame;

import com.twitter.scrooge.Option;
import com.twitter.util.Future;

public class ScrayJdbcTest {

	final int _ROWS = 1000;
	final int _COLS = 50;

	final String _URL = "jdbc:scray:stateless://localhost:18182/myDbSystem/myDbId/myQuerySpace";

	final String _QUERY = "SELECT * FROM myTableId";

	final UUID _UUID = UUID.randomUUID();
	final ScrayUUID _SUUID = new ScrayUUID(_UUID.getLeastSignificantBits(),
			_UUID.getMostSignificantBits());

	final MockedAdapter _MADP = new MockedAdapter();

	final Object[] _VALS = { 1, "foo", 1.3D, true };

	ScrayURL scrayURL;

	ScrayConnection scrayConnection;

	ScrayStatement scrayStatement;

	ScrayTResultFrame frame;

	class MockedAdapter extends ScrayStatelessTServiceAdapter {
		public MockedAdapter() {
			super("127.0.0.1:18182");
			client = mock(ScrayStatelessTService.FutureIface.class);
		}
	}

	void mockStubbing() {
		// stub calls to query
		when(_MADP.getClient().query(any(ScrayTQuery.class))).thenReturn(
				Future.value(_SUUID));
		// stub calls to getResults
		when(_MADP.getClient().getResults(any(ScrayUUID.class), any(Integer.class)))
				.thenReturn(Future.value(frame));
	}

	void createFrame() {
		ScrayTTableInfo tinfo = new ScrayTTableInfo("myDbSystem", "myDbId",
				"myTableId");
		ScrayTQueryInfo qinfo = new ScrayTQueryInfo("myQuerySpace", tinfo,
				new LinkedList<ScrayTColumnInfo>());
		List<ScrayTRow> rlist = new LinkedList<ScrayTRow>();
		for (int j = 1; j <= _ROWS; j++) {
			List<ScrayTColumn> clist = new LinkedList<ScrayTColumn>();
			for (int i = 1; i <= _COLS; i++) {
				clist.add(createColumn(i));
			}
			rlist.add(new ScrayTRow(Option.<ByteBuffer> none(), Option.make(
					true, clist)));
		}
		frame = new ScrayTResultFrame(qinfo, rlist);
	}

	ScrayTColumn createColumn(int col) {
		ScrayTColumnInfo cInfo = new ScrayTColumnInfo("col" + col);
		ByteBuffer bbuf = ByteBuffer.wrap(KryoJavaPoolSerialization
				.getInstance().chill.toBytesWithClass(_VALS[new Random()
				.nextInt(_VALS.length)]));
		ScrayTColumn tCol = new ScrayTColumn(cInfo, bbuf);
		return tCol;
	}

	public ScrayJdbcTest() {
		try {
			scrayURL = new ScrayURL(_URL);
			scrayConnection = new ScrayConnection(scrayURL, _MADP);
			scrayStatement = (ScrayStatement) scrayConnection.createStatement();
		} catch (URISyntaxException ex) {
			ex.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Before
	public void init() {
		createFrame();
		mockStubbing();
	}

	@After
	public void exit() {
		try {
			scrayStatement.close();
			scrayConnection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void jdbcBasicTest() {
		try {
			// get one page of data
			ScrayResultSet scrayResultSet = (ScrayResultSet) scrayStatement
					.executeQuery(_QUERY);
			int rowCount = 0;
			// start position
			assertTrue(scrayResultSet.isBeforeFirst());
			while (scrayResultSet.next()) {
				rowCount++;
				ScrayResultSetMetaData meta = (ScrayResultSetMetaData) scrayResultSet
						.getMetaData();
				for (int col = 1; col <= meta.getColumnCount(); col++) {
					String klass = meta.getColumnClassName(col);
					assertEquals(scrayResultSet.getObject(col).getClass()
							.getName(), klass);
					// try some type-specific accessors (if done wrong, these
					// throw failing the test)
					switch (klass.substring(klass.lastIndexOf('.') + 1)) {
					case "Integer":
						scrayResultSet.getInt(col);
						break;
					case "String":
						scrayResultSet.getString(col);
						break;
					case "Double":
						scrayResultSet.getDouble(col);
						break;
					case "Boolean":
						scrayResultSet.getBoolean(col);
						break;
					default:
						scrayResultSet.getObject(col);
						break;
					}
				}
			}
			// don't miss a row
			assertEquals(rowCount, _ROWS);
			// should be at the end
			assertTrue(scrayResultSet.isAfterLast());
			// and all the way back
			while (scrayResultSet.previous()) {
				rowCount--;
			}
			// back at start
			assertTrue(scrayResultSet.isBeforeFirst());
			scrayResultSet.close();
		} catch (SQLException e) {
			e.printStackTrace();
			// everything should work fine
			fail();
		}
	}

}
