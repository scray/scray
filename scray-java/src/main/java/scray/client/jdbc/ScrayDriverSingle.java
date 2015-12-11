package scray.client.jdbc;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import scray.client.finagle.ScrayCombinedTServiceManager;
import scray.client.finagle.ScrayStatefulTServiceAdapter;
import scray.client.finagle.ScrayStatelessTServiceAdapter;
import scray.client.finagle.ScrayTServiceAdapter;

/**
 * ScrayDriver as singleton.
 *
 */
public class ScrayDriverSingle implements java.sql.Driver {
	
	private static ScrayDriverSingle instance = null;
	private ScrayConnection connection = null;
	
	private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
	
	private ScrayConnection getConnection() {
		return instance.connection;
	}
	
	private void setConnection(ScrayConnection connection) {
		instance.connection = connection;
	}
	
	private static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(ScrayDriver.class);
	
	private ScrayDriverSingle() {
	}
	
	public static ScrayDriverSingle getScrayDriverSingle() {
		if(instance == null) {
			instance = new ScrayDriverSingle();
		}
		return instance;
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		rwLock.readLock().lock();
		try {
			if(getConnection() == null) {
				rwLock.readLock().unlock();
				rwLock.writeLock().lock();
				try {
					if(getConnection() == null) {
						setConnection(generateNewConnection(url, info));
					}
				} finally {
					rwLock.writeLock().unlock();
					rwLock.readLock().lock();
				}
			}
			return getConnection();
		} finally {
			rwLock.readLock().unlock();
		}
	}
	
	/**
	 * create the connection
	 * @param url
	 * @param info
	 * @return
	 * @throws SQLException
	 */
	private ScrayConnection generateNewConnection(String url, Properties info) throws SQLException {
		log.info("Connecting pool with {} connections.", url);
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
