package scray.client.finagle;

import java.sql.SQLException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import scray.client.jdbc.ScrayURL;
import scray.service.qservice.thriftjava.ScrayCombinedStatefulTService;
import scray.service.qservice.thriftjava.ScrayTServiceEndpoint;

import com.google.common.base.Joiner;
import com.twitter.finagle.Thrift;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;

public class ScrayCombinedTServiceManager {

	private org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(ScrayCombinedTServiceManager.class);

	private ScrayCombinedTServiceManager() {
	}

	private static class SingletonHolder {
		static ScrayCombinedTServiceManager instance = new ScrayCombinedTServiceManager();
	}

	public static ScrayCombinedTServiceManager getInstance() {
		return SingletonHolder.instance;
	}

	private class CombinedServiceConnection {
		private ScrayCombinedStatefulTService.FutureIface combinedServiceClient;
		private ScrayURL scrayURL;

		CombinedServiceConnection(ScrayURL scrayURL) {
			this.scrayURL = scrayURL;
		}

		ScrayCombinedStatefulTService.FutureIface getCombinedClient() throws SQLException {
			String[] seedEndpoints = scrayURL.getHostAndPort();
            log.debug("Connecting to endpoints for MetaService");
			for (int i = 0; i < seedEndpoints.length; i++) {
				log.debug("Trying to connect to " + seedEndpoints[i]);
				if (combinedServiceClient == null) {
					combinedServiceClient = Thrift.newIface(seedEndpoints[i],
							ScrayCombinedStatefulTService.FutureIface.class);
				}
				try {
					if (Await.result(combinedServiceClient.ping()))
						return combinedServiceClient;
				} catch (Exception ex) {
					log.warn("Could not connect to seedEndpoint "
							+ seedEndpoints[i], ex);
					combinedServiceClient = null;
				}
			}
			log.error("No seedEndpoint found with valid meta service.");
			throw new SQLException("Could not connect to scray meta service.");
		}
	}

	private List<ScrayTServiceEndpoint> endpointCache = null;
	private CombinedServiceConnection connection = null;

	private java.util.Random rand = new java.util.Random();

	private int TIMEOUT = 10; // secs
	private long REFRESH = 3 * 60 * 1000; // milis

	public void init(ScrayURL scrayURL) {
		// initialize if new, (re)initialize if different, else reuse
		if (connection == null) {
			connection = new CombinedServiceConnection(scrayURL);
		} else if (! connection.scrayURL.equals(scrayURL)) {
			connection = new CombinedServiceConnection(scrayURL);
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
					.getCombinedClient().getServiceEndpoints();
			endpointCache = Await.result(eplist, Duration.fromSeconds(TIMEOUT));
			log.info("Refreshed scray service endpoints: "
					+ Joiner.on(", ").join(endpointCache));
		} catch (Exception e) {
			log.warn("Could not refresh scray service enpoint cache.", e);
		}
	}

	public String getRandomEndpoint() throws SQLException {
		String msg = "Error connecting with BDQ Scray Service: no endpoint available.";

		if (endpointCache == null) {
			log.error(msg);
			throw new SQLException(msg);
		}

		try {
			int nextIdx = rand.nextInt(endpointCache.size());
			ScrayTServiceEndpoint nextEp = endpointCache.get(nextIdx);
			return getHostAndPort(nextEp);
		} catch (Exception e) {
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
