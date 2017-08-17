package scray.client.finagle;

import java.sql.SQLException;

import com.twitter.finagle.Thrift;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;

import scray.service.qmodel.thriftjava.ScrayTQuery;
import scray.service.qmodel.thriftjava.ScrayUUID;
import scray.service.qservice.thriftjava.ScrayCombinedStatefulTService;
import scray.service.qservice.thriftjava.ScrayCombinedStatefulTService.AsyncClient;
import scray.service.qservice.thriftjava.ScrayTResultFrame;

import com.twitter.finagle.builder.ClientBuilder;

class ScrayCombinedStatefulTServiceAdapter implements ScrayTServiceAdapter {

	private ScrayCombinedStatefulTService.AsyncClient client;
	private String endpoint;

	public ScrayCombinedStatefulTService.AsyncClient getClient() {
		// lazy init
		if (client == null) {
			String clientService = new ClientBuilder()
				      .hosts()
				      .stack(Thrift.client)
				      .hostConnectionLimit(1)
				.build()
			client = scray.service.qservice.thriftjava.ScrayCombinedStatefulTService.Client;
			//Thrift.newIface(endpoint,
//					ScrayCombinedStatefulTService.FutureIface.class);
		}
		return client;
	}

	public ScrayCombinedStatefulTServiceAdapter(String endpoint) {
		this.endpoint = endpoint;
	}

	public ScrayUUID query(ScrayTQuery query, int queryTimeout)
			throws SQLException {
		try {
			ScrayUUID fuuid = getClient().query(query);
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
