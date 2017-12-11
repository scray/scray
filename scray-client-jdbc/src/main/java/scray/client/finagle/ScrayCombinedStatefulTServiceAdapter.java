package scray.client.finagle;

import java.sql.SQLException;

import scray.service.qmodel.thriftjava.ScrayTQuery;
import scray.service.qmodel.thriftjava.ScrayUUID;
import scray.service.qservice.thriftjava.ScrayCombinedStatefulTService;
import scray.service.qservice.thriftjava.ScrayTResultFrame;

import com.twitter.finagle.Thrift;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;

public class ScrayCombinedStatefulTServiceAdapter implements ScrayTServiceAdapter {

	private ScrayCombinedStatefulTService.Client client;
	private String endpoint;

	public ScrayCombinedStatefulTService.Client getClient() {
		// lazy init
		if (client == null) {
			client = Thrift.client().newIface(endpoint,
					ScrayCombinedStatefulTService.Client.class);
		}
		return client;
	}

	public ScrayCombinedStatefulTServiceAdapter(String endpoint) {
		this.endpoint = endpoint;
	}

	public ScrayUUID query(ScrayTQuery query, int queryTimeout)
			throws SQLException {
		try {
			return getClient().query(query);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public ScrayTResultFrame getResults(ScrayUUID queryId, int queryTimeout)
			throws SQLException {
		try {
			return getClient().getResults(queryId);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
}