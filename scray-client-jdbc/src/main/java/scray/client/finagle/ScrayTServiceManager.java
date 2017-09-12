//package scray.client.finagle;
//
//import java.sql.SQLException;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Timer;
//import java.util.TimerTask;
//import java.util.concurrent.locks.ReentrantReadWriteLock;
//
//import com.google.common.base.Joiner;
//import com.twitter.finagle.Thrift;
//import com.twitter.util.Await;
//import com.twitter.util.Duration;
//import com.twitter.util.Future;
//
//import scray.client.jdbc.ScrayURL;
//import scray.service.qservice.thriftjava.ScrayMetaTService;
//import scray.service.qservice.thriftjava.ScrayTServiceEndpoint;
//
//public class ScrayTServiceManager {
//
//	private org.slf4j.Logger log = org.slf4j.LoggerFactory
//			.getLogger(ScrayTServiceManager.class);
//
//	private ScrayTServiceManager() {
//	}
//
//	private static class SingletonHolder {
//		static ScrayTServiceManager instance = new ScrayTServiceManager();
//	}
//
//	public static ScrayTServiceManager getInstance() {
//		return SingletonHolder.instance;
//	}
//
//	private class MetaServiceConnection {
//		// private ScrayMetaTService.Client.FutureIface metaServiceClient;
//		private ScrayMetaTService.AsyncClient metaServiceClient;
//
//		private ScrayURL scrayURL;
//
//		MetaServiceConnection(ScrayURL scrayURL) {
//			this.scrayURL = scrayURL;
//		}
//
//		ScrayMetaTService.AsyncClient getMetaClient() throws SQLException {
//			String[] seedEndpoints = scrayURL.getHostAndPort();
//			for (int i = 0; i < seedEndpoints.length; i++) {
//				log.trace("Trying to connect to " + seedEndpoints[i]);
//				if (metaServiceClient == null) {
//					metaServiceClient = Thrift..; //(seedEndpoints[i],
//							ScrayMetaTService.AsyncClient.class);
//					try {
//						if (Await.result(metaServiceClient.ping()))
//							return metaServiceClient;
//					} catch (Exception ex) {
//						log.warn("Could not connect to seedEndpoint "
//								+ seedEndpoints[i], ex);
//						metaServiceClient = null;
//					}
//				}
//			}
//			log.error("No seedEndpoint found with valid meta service.");
//			throw new SQLException("Could not connect to scray meta service.");
//		}
//	}
//
//	private static Map<String, List<ScrayTServiceEndpoint>> endpointCache = new HashMap<String, List<ScrayTServiceEndpoint>>();
//
//	private static Map<String, MetaServiceConnection> connections = new HashMap<String, MetaServiceConnection>();
//	private static ReentrantReadWriteLock connectionLock = new ReentrantReadWriteLock(); 
//	
//	private ScrayURL scrayURL = null;
//	
//	private java.util.Random rand = new java.util.Random();
//
//	private int TIMEOUT = 10; // secs
//	private long REFRESH = 3 * 60 * 1000; // milis
//
//	public void init(final ScrayURL scrayURL) {
//		this.scrayURL = scrayURL;
//		// initialize if new, (re)initialize if different, else reuse
//		connectionLock.writeLock().lock();
//		try {
//			if (connections.get(scrayURL.toString()) == null) {
//				connections.put(scrayURL.toString(), new MetaServiceConnection(scrayURL));
//				TimerTask tt = new java.util.TimerTask() {
//					private String url = scrayURL.toString();
//					@Override
//					public void run() {
//						refreshEndpoints(url);
//					}
//				};
//				// schedule refresh of endpointCache (as daemon)
//				Timer t = new Timer(true);
//				t.scheduleAtFixedRate(tt, REFRESH, REFRESH);
//			} 
//			
//			// initially fill the endpoint cache
//			refreshEndpoints(scrayURL.toString());
//	
//		} finally {
//			connectionLock.writeLock().unlock();
//		}
//	}
//
//	void refreshEndpoints(String url) {
//		connectionLock.writeLock().lock();
//		try {
//			try {
//				Future<List<ScrayTServiceEndpoint>> eplist = connections.get(url)
//						.getMetaClient().getServiceEndpoints();
//				List<ScrayTServiceEndpoint> endpoints = Await.result(eplist, Duration.fromSeconds(TIMEOUT));
//				endpointCache.put(url, endpoints);
//				log.debug("Refreshed scray service endpoints: "
//						+ Joiner.on(", ").join(endpoints));
//			} catch (Exception e) {
//				log.warn("Could not refresh scray service enpoint cache.", e);
//			}
//		} finally {
//			connectionLock.writeLock().unlock();
//		}
//	}
//
//	public String getRandomEndpoint() throws SQLException {
//		connectionLock.readLock().lock();
//		try {
//			String msg = "Error connecting with BDQ Scray Service: no endpoint available.";
//	
//			if (endpointCache == null || scrayURL == null || endpointCache.get(scrayURL.toString()) == null) {
//				log.error(msg);
//				throw new SQLException(msg);
//			}
//	
//			try {
//				int nextIdx = rand.nextInt(endpointCache.size());
//				ScrayTServiceEndpoint nextEp = endpointCache.get(scrayURL.toString()).get(nextIdx);
//				return getHostAndPort(nextEp);
//			} catch (Exception e) {
//				log.error(msg, e);
//				throw new SQLException(msg);
//			}
//		} finally {
//			connectionLock.readLock().unlock();
//		}
//	}
//
//	private String getHostAndPort(ScrayTServiceEndpoint endpoint) {
//		String DELIMITER = ":";
//		return endpoint.getHost() + DELIMITER + endpoint.getPort();
//	}
//
//	public List<ScrayTServiceEndpoint> getEndpointCache() {
//		connectionLock.readLock().lock();
//		try {
//			return (endpointCache == null || scrayURL == null || endpointCache.get(scrayURL.toString()) == null)?null:endpointCache.get(scrayURL.toString());
//		} finally {
//			connectionLock.readLock().unlock();
//		}
//	}
//
//	public ScrayURL getScrayURL() {
//		return scrayURL;
//	}
//
//	public boolean isStatefulTService() {
//		return scrayURL.getProtocolMode().equals(
//				ScrayURL.ProtocolModes.stateful.name());
//	}
//
//	public boolean isStatelessTService() {
//		return scrayURL.getProtocolMode().equals(
//				ScrayURL.ProtocolModes.stateless.name());
//	}
//}
