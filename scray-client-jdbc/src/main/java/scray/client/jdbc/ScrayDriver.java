package scray.client.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import scray.common.properties.PropertyException;
import scray.common.properties.ScrayProperties;
import scray.common.properties.ScrayProperties.Phase;
import scray.common.properties.predefined.PredefinedProperties;

/**
 * Wrapper to ensure that one instance of the JDBC driver exists.
 */
public class ScrayDriver implements java.sql.Driver {
	private ScrayDriverSingle instance = null;
	
	public ScrayDriver() {
		instance = ScrayDriverSingle.getScrayDriverSingle();
	}
	
	private static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ScrayDriver.class);


	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		return instance.connect(url, info);
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return instance.acceptsURL(url);
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return instance.getPropertyInfo(url, info);
	}

	@Override
	public int getMajorVersion() {
		return instance.getMajorVersion();
	}

	@Override
	public int getMinorVersion() {
		return instance.getMinorVersion();
	}

	@Override
	public boolean jdbcCompliant() {
		return instance.jdbcCompliant();
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return instance.getParentLogger();
	}
	
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
}
