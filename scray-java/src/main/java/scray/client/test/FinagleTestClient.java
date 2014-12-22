package scray.client.test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import scala.runtime.BoxedUnit;
import scray.client.finagle.FinagleThriftConnection;
import scray.service.qmodel.thriftjava.ScrayTColumnInfo;
import scray.service.qmodel.thriftjava.ScrayTQuery;
import scray.service.qmodel.thriftjava.ScrayTQueryInfo;
import scray.service.qmodel.thriftjava.ScrayTRow;
import scray.service.qmodel.thriftjava.ScrayTTableInfo;
import scray.service.qmodel.thriftjava.ScrayUUID;
import scray.service.qservice.thriftjava.ScrayTResultFrame;

import com.twitter.scrooge.Option;
import com.twitter.util.Await;
import com.twitter.util.Function;
import com.twitter.util.Future;

public class FinagleTestClient {

	public static void main(String[] args) throws Exception {

		FinagleThriftConnection con = new FinagleThriftConnection("localhost:18181");
		
		// prepare a query
		List<ScrayTColumnInfo> clist = new LinkedList<ScrayTColumnInfo>();

		for (int i = 1; i < 3; i++) {
			clist.add(new ScrayTColumnInfo("col" + i));
		}

		ScrayTTableInfo tinfo = new ScrayTTableInfo("foo", "bar", "baz");
		ScrayTQueryInfo qinfo = new ScrayTQueryInfo(Option.<ScrayUUID> none(),
				"bla", tinfo, clist, Option.make(true, 2), Option.<Long> none());
		ScrayTQuery query = new ScrayTQuery(qinfo,
				new HashMap<String, ByteBuffer>(),
				"SELECT col1, col2 FROM @baz");

		Future<ScrayUUID> quid = con.getScrayTService().query(query).onSuccess(
				new Function<ScrayUUID, BoxedUnit>() {
					@Override
					public BoxedUnit apply(ScrayUUID response) {
						System.out.println("Received response: " + response);
						return null;
					}
				});

		ScrayUUID quidok = Await.result(quid);

		boolean hasNextFrame = true;
		int rowcount = 0;

		while (hasNextFrame) {

			Future<ScrayTResultFrame> frame = con.getScrayTService().getResults(quidok)
					.onSuccess(new Function<ScrayTResultFrame, BoxedUnit>() {
						@Override
						public BoxedUnit apply(ScrayTResultFrame response) {
							System.out
									.println("Received response: " + response);
							return null;
						}
					});

			ScrayTResultFrame frameok = Await.result(frame);

			List<ScrayTRow> rows = frameok.getRows();

			for (Iterator<ScrayTRow> iterator = rows.iterator(); iterator
					.hasNext();) {
				ScrayTRow scrayTRow = (ScrayTRow) iterator.next();
				if (!scrayTRow.isSetColumns()) {
					hasNextFrame = false;
				} else {
					rowcount++;
					System.out.println("Row (" + rowcount + ") : " + scrayTRow);
				}
			}

		}
	}
}
