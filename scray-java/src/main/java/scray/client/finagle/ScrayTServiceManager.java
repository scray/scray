package scray.client.finagle;

import java.sql.SQLException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import scray.client.jdbc.ScrayURL;
import scray.service.qservice.thriftjava.ScrayMetaTService;
import scray.service.qservice.thriftjava.ScrayTServiceEndpoint;

import com.google.common.base.Joiner;
import com.twitter.finagle.Thrift;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;

public class ScrayTServiceManager {

	private org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(ScrayTServiceManager.class);

	private ScrayTServiceManager() {
	}

	private static class SingletonHolder {
		static ScrayTServiceManager instance = new ScrayTServiceManager();
	}

	public static ScrayTServiceManager getInstance() {
		return SingletonHolder.instance;
	}

	private class MetaServiceConnection {
		private ScrayMetaTService.FutureIface metaServiceClient;
		private ScrayURL scrayURL;

		MetaServiceConnection(ScrayURL scrayURL) {
			this.scrayURL = scrayURL;
		}

		String seedEndpoint() {
			return scrayURL.getHostAndPort();
		}

		ScrayMetaTService.FutureIface getMetaClient() {
			if (metaServiceClient == null) {
				metaServiceClient = Thrift.newIface(seedEndpoint(),
						ScrayMetaTService.FutureIface.class);
			}
			return metaServiceClient;
		}
	}

	private List<ScrayTServiceEndpoint> endpointCache = null;
	private MetaServiceConnection connection = null;

	private java.util.Random rand = new java.util.Random();

	private int TIMEOUT = 10; // secs
	private long REFRESH = 3 * 60 * 1000; // milis

	public void init(ScrayURL scrayURL) {
		// initialize if new, (re)initialize if different, else reuse
		if (connection == null) {
			connection = new MetaServiceConnection(scrayURL);
		} else if (connection.scrayURL.equals(scrayURL)) {
			connection = new MetaServiceConnection(scrayURL);
		}

		// initially fill the endpoint cache
		refreshEndpoints();

		TimerTask tt = new java.util.TimerTask() {			
			@Override
			public void run() {
				refreshEndpoints();
			}
		};

		// schedule refresh of endpointCache (as daemon)
		Timer t = new Timer(true);
		t.scheduleAtFixedRate(tt, REFRESH, REFRESH);
	}

	void refreshEndpoints() {
		try {
			Future<List<ScrayTServiceEndpoint>> eplist = connection
					.getMetaClient().getServiceEndpoints();
			endpointCache = Await.result(eplist, Duration.fromSeconds(TIMEOUT));
			log.info("Refreshed scray service endpoints: "
					+ Joiner.on(", ").join(endpointCache));
		} catch (Exception e) {
			log.error("Error refreshing scray service enpoint cache.", e);
		}
	}

	public String getRandomEndpoint() throws SQLException {
		try {
			int nextIdx = rand.nextInt(endpointCache.size());
			ScrayTServiceEndpoint nextEp = endpointCache.get(nextIdx);
			return getHostAndPort(nextEp);
		} catch (Exception e) {
			String msg = "Error connecting with BDQ Scray Service: no endpoint available.";
			log.error(msg, e);
			throw new SQLException(msg);
		}
	}

	private String getHostAndPort(ScrayTServiceEndpoint endpoint) {
		String DELIMITER = ":";
		return endpoint.getHost() + DELIMITER + endpoint.getPort();
	}

	public List<ScrayTServiceEndpoint> getEndpointCache() {
		return endpointCache;
	}

	public ScrayURL getScrayURL() {
		return connection.scrayURL;
	}

	public boolean isStatefulTService() {
		return connection.scrayURL.getProtocolMode().equals(
				ScrayURL.ProtocolModes.stateful.name());
	}

	public boolean isStatelessTService() {
		return connection.scrayURL.getProtocolMode().equals(
				ScrayURL.ProtocolModes.stateless.name());
	}
}
