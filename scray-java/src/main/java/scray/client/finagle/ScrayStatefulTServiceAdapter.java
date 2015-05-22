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
	private String endpoint;

	public ScrayStatefulTService.FutureIface getClient() {
		// lazy init
		if (client == null) {
			client = Thrift.newIface(endpoint,
					ScrayStatefulTService.FutureIface.class);
		}
		return client;
	}

	public ScrayStatefulTServiceAdapter(String endpoint) {
		this.endpoint = endpoint;
	}

	public ScrayUUID query(ScrayTQuery query, int queryTimeout)
			throws SQLException {
		try {
			Future<ScrayUUID> fuuid = getClient().query(query);
			return Await.result(fuuid, Duration.fromSeconds(queryTimeout));
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public ScrayTResultFrame getResults(ScrayUUID queryId, int queryTimeout)
			throws SQLException {
		try {
			Future<ScrayTResultFrame> fframe = getClient().getResults(queryId);
			ScrayTResultFrame frame = Await.result(fframe,
					Duration.fromSeconds(queryTimeout));
			return frame;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
}
