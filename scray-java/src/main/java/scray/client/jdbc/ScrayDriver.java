package scray.client.jdbc;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import scray.client.finagle.ScrayStatefulTServiceAdapter;
import scray.client.finagle.ScrayStatelessTServiceAdapter;
import scray.client.finagle.ScrayTServiceAdapter;
import scray.client.finagle.ScrayTServiceManager;
import scray.common.properties.PropertyException;
import scray.common.properties.ScrayProperties;
import scray.common.properties.ScrayProperties.Phase;
import scray.common.properties.predefined.PredefinedProperties;

public class ScrayDriver implements java.sql.Driver {

	private static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(ScrayDriver.class);

	static {
		try {
			try {
				ScrayProperties
						.registerProperty(PredefinedProperties.RESULT_COMPRESSION_MIN_SIZE);
				ScrayProperties.setPhase(Phase.config);
				ScrayProperties.setPhase(Phase.use);
			} catch (PropertyException p) {
				throw new RuntimeException(p);
			}
			// Register the ScrayDriver with DriverManager
			ScrayDriver driverInst = new ScrayDriver();
			DriverManager.registerDriver(driverInst);
			// System.setSecurityManager(new RMISecurityManager());

		} catch (SQLException e) {
			log.error("Error registering jdbc driver.", e);
		}
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		try {
			if (acceptsURL(url)) {
				ScrayURL scrayURL = new ScrayURL(url);
				ScrayTServiceManager tManager = ScrayTServiceManager
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
