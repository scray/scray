package scray.client.jdbc;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import scray.client.finagle.ScrayStatefulTServiceAdapter;
import scray.client.finagle.ScrayStatelessTServiceAdapter;
import scray.client.finagle.ScrayTServiceAdapter;
import scray.client.finagle.ScrayCombinedTServiceManager;

/**
 * ScrayDriver as singleton.
 *
 */
public class ScrayDriverSingle implements java.sql.Driver {
	
	private static ScrayDriverSingle instance = null;
	private ScrayConnection[] connectionPool = null;
	
	private int connectionPoolSize = 10;
	
	private static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(ScrayDriver.class);
	
	private ScrayDriverSingle() {
		this.connectionPool = new ScrayConnection[connectionPoolSize];
	}
	
	public static ScrayDriverSingle getScrayDriverSingle() {
		if(instance == null) {
			instance = new ScrayDriverSingle();
		}
		return instance;
	}

//	static {
//		try {
//			try {
//				ScrayProperties
//						.registerProperty(PredefinedProperties.RESULT_COMPRESSION_MIN_SIZE);
//				ScrayProperties.setPhase(Phase.config);
//				ScrayProperties.setPhase(Phase.use);
//			} catch (PropertyException p) {
//				throw new RuntimeException(p);
//			}
//			// Register the ScrayDriver with DriverManager
//			ScrayDriver driverInst = new ScrayDriver();
//			DriverManager.registerDriver(driverInst);
//			// System.setSecurityManager(new RMISecurityManager());
//
//		} catch (SQLException e) {
//			log.error("Error registering jdbc driver.", e);
//		}
//	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		ScrayConnection selectedConnection = null;

		// Fill pool if it is the first star
		if(connectionPool[0] == null) {
			log.info("Fill connection pool with {} connections.", connectionPoolSize);
			for (int i = 0; i < connectionPool.length; i++) {
				connectionPool[i] = new ScrayConnection(null, null); //generateNewConnection(url, info);
			}
		}
		
		for (int i = 0; i < connectionPool.length && selectedConnection == null; i++) {
		
			// Return free connection
			if(!connectionPool[i].isInUse()) {
				connectionPool[i].setInUse(true);
				selectedConnection = connectionPool[i];
			}
			
			// Wait for a free connection
			if(i == (connectionPoolSize-1) &&  connectionPool[i].isInUse()) {
				long waitingTime = System.currentTimeMillis();
				
				while(selectedConnection == null) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						log.debug("Interupt while waiting for a free connection. No problem");
					}

					// Return free connection
					for (int j = 0; j < connectionPool.length; j++) {
						if (!connectionPool[j].isInUse()) {
							log.info("Found free connection after {} ms", System.currentTimeMillis() - waitingTime);
							connectionPool[j].setInUse(true);
							selectedConnection = connectionPool[j];
						}
					}
				}
			}
			
		}
		
		return selectedConnection;
	}
	
	private ScrayConnection generateNewConnection(String url, Properties info) throws SQLException {
		try {
			if (acceptsURL(url)) {
				ScrayURL scrayURL = new ScrayURL(url);
				ScrayCombinedTServiceManager tManager = ScrayCombinedTServiceManager
						.getInstance();
				tManager.init(scrayURL);
				ScrayTServiceAdapter tAdapter = null;
				try {
					if (tManager.isStatefulTService()) {
						tAdapter = new ScrayStatefulTServiceAdapter(
								tManager.getRandomEndpoint());
					} else {
						tAdapter = new ScrayStatelessTServiceAdapter(
								tManager.getRandomEndpoint());
					}
				} catch (Exception e) {
					String msg = "Error setting up scray connection.";
					log.error(msg, e);
					throw new SQLException(msg);
				}
				return new ScrayConnection(scrayURL, tAdapter);
			} else {
				return null;
			}
		} catch (URISyntaxException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		try {
			new ScrayURL(url);
		} catch (URISyntaxException e) {
			return false;
		}
		return true;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMajorVersion() {
		return 1;
	}

	@Override
	public int getMinorVersion() {
		return 0;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}

}
