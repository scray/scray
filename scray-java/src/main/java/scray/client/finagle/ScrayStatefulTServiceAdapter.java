package scray.client.finagle;

import java.sql.SQLException;

import scray.service.qmodel.thriftjava.ScrayTQuery;
import scray.service.qmodel.thriftjava.ScrayUUID;
import scray.service.qservice.thriftjava.ScrayStatefulTService;
import scray.service.qservice.thriftjava.ScrayStatelessTService;
import scray.service.qservice.thriftjava.ScrayTResultFrame;

import com.twitter.finagle.Thrift;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;

public class ScrayStatefulTServiceAdapter implements ScrayTServiceAdapter {

	private ScrayStatefulTService.FutureIface client;

	public ScrayStatefulTService.FutureIface getClient() {
		return client;
	}

	public ScrayStatefulTServiceAdapter(String endpoint) {
		client = Thrift.newIface(endpoint,
				ScrayStatelessTService.FutureIface.class);
	}

	public ScrayUUID query(ScrayTQuery query, int queryTimeout)
			throws SQLException {
		try {
			Future<ScrayUUID> fuuid = client.query(query);
			return Await
					.result(fuuid, new Duration(queryTimeout * 1000000000L));
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public ScrayTResultFrame getResults(ScrayUUID queryId, int queryTimeout)
			throws SQLException {
		try {
			Future<ScrayTResultFrame> fframe = client.getResults(queryId);
			ScrayTResultFrame frame = Await.result(fframe, new Duration(
					queryTimeout * 1000000000L));
			return frame;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
}
