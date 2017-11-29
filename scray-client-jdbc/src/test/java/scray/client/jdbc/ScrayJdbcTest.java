//// See the LICENCE.txt file distributed with this work for additional
//// information regarding copyright ownership.
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
//// http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//package scray.client.jdbc;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.fail;
//import static org.mockito.Matchers.any;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//import java.net.URISyntaxException;
//import java.nio.ByteBuffer;
//import java.sql.SQLException;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Random;
//import java.util.UUID;
//
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//import org.xerial.snappy.Snappy;
//
//import scray.client.finagle.ScrayStatelessTServiceAdapter;
//import scray.common.properties.PropertyException;
//import scray.common.properties.ScrayProperties;
//import scray.common.properties.ScrayProperties.Phase;
//import scray.common.properties.predefined.PredefinedProperties;
//import scray.common.serialization.pool.KryoJavaPoolSerialization;
//import scray.service.qmodel.thriftjava.ScrayTColumn;
//import scray.service.qmodel.thriftjava.ScrayTColumnInfo;
//import scray.service.qmodel.thriftjava.ScrayTQuery;
//import scray.service.qmodel.thriftjava.ScrayTQueryInfo;
//import scray.service.qmodel.thriftjava.ScrayTRow;
//import scray.service.qmodel.thriftjava.ScrayTTableInfo;
//import scray.service.qmodel.thriftjava.ScrayUUID;
//import scray.service.qservice.thriftjava.ScrayStatelessTService;
//import scray.service.qservice.thriftjava.ScrayTException;
//import scray.service.qservice.thriftjava.ScrayTResultFrame;
//
//import com.esotericsoftware.minlog.Log;
//import com.twitter.scrooge.Option;
//import com.twitter.util.Future;
//
//public class ScrayJdbcTest {
//
//	final int _ROWS = 1000;
//	final int _COLS = 50;
//
//	final String _URL = "jdbc:scray:stateless://localhost:18182/myDbSystem/myDbId/myQuerySpace";
//
//	final String _QUERY = "SELECT * FROM myTableId";
//
//	final UUID _UUID = UUID.randomUUID();
//	final ScrayUUID _SUUID = new ScrayUUID(_UUID.getLeastSignificantBits(),
//			_UUID.getMostSignificantBits());
//
//	final MockedAdapter _MADP = new MockedAdapter();
//
//	final Object[] _VALS = {
//			1,
//			"foo",
//			1.3D,
//			true,
//			"NssigZXQxSnh4n28G4Hn BNHnzCWLuUbRrrhIGjFd DhMyCkQ1BrSD0PzwEIsd icT3t8Rs8RIFMCvwcAme nLmOhDSq5OpG8yZWxn5e 5onb0ZfFW0OkfcEQy9eQ naTF8Vi0rrtl5genhUUY zoBT2LjewhPOTQBhMIhQ D5javQm2ftJlSmB2zVKQ PGQ0ZV11O6FWkz9hu4fr hq47t7tx5ZuD8qNKS8pD 25FrTVbCS8y6ngIEOZEu i4FHtRYuZY24iiuEGSa1 NsXxrtr5xET08RXVZvfJ TzE8vr8dtncqObcdOK5k a3I4ZAo1p3YsEBrSTTG6 o54WDLBICtTvosQ0c4f9 rQ0himrKoBNzbUMEfZON S8bpl7LXUyiFkSSSi2nM RXpGA6A4ZPejGmuoq5qZ fwMswNb84cW3u61KjA0R Q0Al127Jpaaw9l8rykLe pFf9VM2vux0zSKqdzLrc Vi9X6iAEb90Pmj1BDbyM qu28FKf5QNdAQ7QUDUAl KUBA0r0szIPKPibvEdvv 7CvVz6gWvIh0BvYvWv4c lh4q7gJUmOxjE2zTbQmp aZ2Me7ys8yNjwgv9ZQsR mThY5Yc04sEXvSKVrdJP lsyXmq1YmQXgL0XrgkzJ Q2CJ4qb01OVr9aeMYr3U Lm1mPTTYFtja7cqEyxo0 ljpYn1zCtHHTcnoX2pNE yUBC6sPmAyeZETLzXXrz ikcn14zWy48DqvrlGMZV h9c6IiPAEcxT1iUCTJZa vvczYML02zsEPWPzzZYe LGgdW7bPXK8DLvBtBXjI 0vZbYZ36MMiE7NcEwXFb KyKQmEKU4RJXy2jcDAYx RP0VfFZMIxHx60I1LhEL tt9lrfse0Qir573NOHxe yaLXUduFkZJdXchHiZSt RwdR7z8QFqMo1WWITsgq gnfko0rqE880jMRZGfvp FZOxRkrYgYv7WkZ6XwGM 18OvJxnMQkuGjczcIzW6 CGrw4ds8jxEGZaS94hqw AEeV5gPi3Gjzxt2qkKYf OrclwOSQllLfljfXkhL4 iKWMBo3B5Fm2iBvAS9od BhISBcxxmeCa7SO74LBO Lp3yr7bmcjKQTk21i518 4aN92xQh889qKgu2MnVJ RYcrC8W1ocsXDOxKf42G 7Z63MiJcs5XD102c8hmK mDc9MT0mM546pw2foZOF DCBWgSFD5QtW30cToTId jmMk2CMm9N946e9KkGza OyLWddFsgINqO27q8RQv fnNDXtbXnBXkhiD9JtIc vftjLttDQocvolzJEn9r xThBpZo8O8QTfMOl63dM hHKwECT8hkX8hMqaHQ3M 8prJXokfc1jfHKOshccQ GmRRmUqP67D7ScA2xjCh AmrCEMXDSBtXYzhhY59o ua85XSUn4NRd4kLOSHf8 WcMVea0DxXbMQgiF63pF YYZPC94VCxY7HrTbja7t qgpaeBg8Ycmjx2vADgDL Nm4qOUr1ni76jszIul4O JNHyQwf4d4lGZ1bQEE2f 4Zwa14kWoT7Z5jgoQHdG ZMHQARUV1hfNRmRHYmpm b16150Alrx115fU4iaVp TpdUHU6jSc0uZH3FYTWX hIoZkBqFMjXDSTUIHZeY gdvNnAzVfPvVzVb2Ngs2 pxopnW7CU2H8ZxPDsJyt aOc9LJtQHUvRv9nZ9H09 BEBmYvWXIICr6VJV3XjY wCT9hVHQthM8VXN2AUOc 1luAmNlGxFbPdFsNNYKB jcSYKhymF2m6Tz03FpHK ZZFeFtpVVsHEmwJLlAgy AwslBGMEvvdwa0kQA7y6 vs8u6MV6Yvm0HMyAutQ9 OpT0NcxgvEKMnMcT6An1 AhGZX6w6ai74DCTu2cHf x6OoZHhq11W6YzRSGpjf 1x2V8wJ6R3pqy9yTt5zn hZprDkB1n8LWEkI2kxBn Uu6RqQnJqTMQQS7mKtCr dWi2lazPcmLpSelaIdBb khBXYs4peG5LGYW8Uhss nrVE4vo9wxhIXVqGrfM7 zNIakxgj2gAWTtocLXWK lrKEZaTi94GHdQdOkt2W" };
//
//	ScrayURL scrayURL;
//
//	ScrayConnection scrayConnection;
//
//	ScrayStatement scrayStatement;
//
//	ScrayTResultFrame frame;
//
//	class MockedAdapter extends ScrayStatelessTServiceAdapter {
//		public MockedAdapter() {
//			super("127.0.0.1:18182");
//			client = mock(ScrayStatelessTService.FutureIface.class);
//		}
//	}
//
//	void mockStubbing() throws ScrayTException {
//		// stub calls to query
//		when(_MADP.getClient().query(any(ScrayTQuery.class))).thenReturn(
//				Future.value(_SUUID));
//		// stub calls to getResults
//		when(
//				_MADP.getClient().getResults(any(ScrayUUID.class),
//						any(Integer.class))).thenReturn(Future.value(frame));
//	}
//
//	void createFrame() {
//		ScrayTTableInfo tinfo = new ScrayTTableInfo("myDbSystem", "myDbId",
//				"myTableId");
//		ScrayTQueryInfo qinfo = new ScrayTQueryInfo("myQuerySpace", tinfo,
//				new LinkedList<ScrayTColumnInfo>());
//		List<ScrayTRow> rlist = new LinkedList<ScrayTRow>();
//		for (int j = 1; j <= _ROWS; j++) {
//			List<ScrayTColumn> clist = new LinkedList<ScrayTColumn>();
//			for (int i = 1; i <= _COLS; i++) {
//				clist.add(createColumn(i));
//			}
//			rlist.add(new ScrayTRow(Option.<ByteBuffer> none(), Option.make(
//					true, clist)));
//		}
//		frame = new ScrayTResultFrame(qinfo, rlist);
//	}
//
//	ScrayTColumn createColumn(int col) {
//		ScrayTColumn tCol = null;
//		try {
//			ScrayTColumnInfo cInfo = new ScrayTColumnInfo("col" + col);
//			byte[] rawbytes = KryoJavaPoolSerialization.getInstance().chill
//					.toBytesWithClass(_VALS[new Random().nextInt(_VALS.length)]);
//			byte[] bytes = rawbytes;
//			if (rawbytes.length >= ScrayProperties
//					.getPropertyValue(PredefinedProperties.RESULT_COMPRESSION_MIN_SIZE)) {
//				bytes = Snappy.compress(rawbytes);
//			}
//			ByteBuffer bbuf = ByteBuffer.wrap(bytes);
//			tCol = new ScrayTColumn(cInfo, bbuf);
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			new RuntimeException(ex);
//		}
//		return tCol;
//	}
//
//	// ScrayTColumn createColumn(int col) {
//	// ScrayTColumn tCol = null;
//	// try {
//	// ScrayTColumnInfo cInfo = new ScrayTColumnInfo("col" + col);
//	// byte[] bytes = KryoJavaPoolSerialization.getInstance().chill
//	// .toBytesWithClass(_VALS[new Random().nextInt(_VALS.length)]);
//	// ByteBuffer bbuf = ByteBuffer.wrap(bytes);
//	// tCol = new ScrayTColumn(cInfo, bbuf);
//	// } catch (Exception ex) {
//	// ex.printStackTrace();
//	// new RuntimeException(ex);
//	// }
//	// return tCol;
//	// }
//
//	// ScrayTColumn createColumn(int col) {
//	// ScrayTColumn tCol = null;
//	// try {
//	// ScrayTColumnInfo cInfo = new ScrayTColumnInfo("col" + col);
//	// ByteArrayOutputStream baos = new ByteArrayOutputStream();
//	// GZIPOutputStream gzip = new GZIPOutputStream(baos);
//	// gzip.write(KryoJavaPoolSerialization.getInstance().chill
//	// .toBytesWithClass(_VALS[new Random().nextInt(_VALS.length)]));
//	// gzip.flush();
//	// gzip.close();
//	// ByteBuffer bbuf = ByteBuffer.wrap(baos.toByteArray());
//	// tCol = new ScrayTColumn(cInfo, bbuf);
//	// } catch (Exception ex) {
//	// ex.printStackTrace();
//	// new RuntimeException(ex);
//	// }
//	// return tCol;
//	// }
//
//	public ScrayJdbcTest() {
//		try {
//			scrayURL = new ScrayURL(_URL);
//			scrayConnection = new ScrayConnection(scrayURL, _MADP);
//			scrayStatement = (ScrayStatement) scrayConnection.createStatement();
//		} catch (URISyntaxException ex) {
//			ex.printStackTrace();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//	}
//
//	@Before
//	public void init() throws ScrayTException {
//		try {
//			ScrayProperties
//					.registerProperty(PredefinedProperties.RESULT_COMPRESSION_MIN_SIZE);
//			ScrayProperties.setPhase(Phase.config);
//			ScrayProperties.setPhase(Phase.use);
//		} catch (PropertyException pe) {
//			Log.error("init error", pe);
//		}
//		createFrame();
//		mockStubbing();
//	}
//
//	@After
//	public void exit() {
//		try {
//			scrayStatement.close();
//			scrayConnection.close();
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//	}
//
//	@Test
//	public void jdbcBasicTest() {
//		try {
//			// get one page of data
//			ScrayResultSet scrayResultSet = (ScrayResultSet) scrayStatement
//					.executeQuery(_QUERY);
//			int rowCount = 0;
//			// start position
//			assertTrue(scrayResultSet.isBeforeFirst());
//			while (scrayResultSet.next()) {
//				rowCount++;
//				ScrayResultSetMetaData meta = (ScrayResultSetMetaData) scrayResultSet
//						.getMetaData();
//				for (int col = 1; col <= meta.getColumnCount(); col++) {
//					String klass = meta.getColumnClassName(col);
//					assertEquals(scrayResultSet.getObject(col).getClass()
//							.getName(), klass);
//					// try some type-specific accessors (if done wrong, these
//					// throw failing the test)
//					switch (klass.substring(klass.lastIndexOf('.') + 1)) {
//					case "Integer":
//						scrayResultSet.getInt(col);
//						break;
//					case "String":
//						scrayResultSet.getString(col);
//						break;
//					case "Double":
//						scrayResultSet.getDouble(col);
//						break;
//					case "Boolean":
//						scrayResultSet.getBoolean(col);
//						break;
//					default:
//						scrayResultSet.getObject(col);
//						break;
//					}
//				}
//			}
//			// don't miss a row
//			assertEquals(rowCount, _ROWS);
//			// should be at the end
//			assertTrue(scrayResultSet.isAfterLast());
//			// and all the way back
//			while (scrayResultSet.previous()) {
//				rowCount--;
//			}
//			// back at start
//			assertTrue(scrayResultSet.isBeforeFirst());
//			scrayResultSet.close();
//		} catch (SQLException e) {
//			e.printStackTrace();
//			// everything should work fine
//			fail();
//		}
//	}
//	
//}
