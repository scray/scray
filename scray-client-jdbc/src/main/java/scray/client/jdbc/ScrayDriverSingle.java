package scray.client.jdbc;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Date;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
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
	
	private ReentrantLock rwLock = new ReentrantLock();
	
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

	private class ConnectionTimeoutTask extends TimerTask {

		private Thread originalThread;
		private AtomicBoolean finished = new AtomicBoolean(false);
		
		private ConnectionTimeoutTask(Thread originalThread) {
			this.originalThread = originalThread;
		}
		
		@Override
		public void run() {
			if(!finished.get()) {
				originalThread.interrupt();
			}
		}
		
		public void finish() {
			finished.set(true);
		}
	}
	
	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		rwLock.lock();
		try {
			if(getConnection() == null || getConnection().getIsFailed().get()) {
				Timer connectionTimeoutTimer = new Timer();
				ConnectionTimeoutTask tt = new ConnectionTimeoutTask(Thread.currentThread());
				connectionTimeoutTimer.schedule(tt, new Date(System.currentTimeMillis() + 120000));
				setConnection(generateNewConnection(url, info));
				tt.finish();
				connectionTimeoutTimer.cancel();
			}
			return getConnection();
		} finally {
			rwLock.unlock();
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
				try {
					tManager.init(scrayURL);
				} catch (Exception e) {
					if(connection != null) {
						connection.setIsFailed(new AtomicBoolean(true));
					}
					throw new SQLException(e); 
				}
				
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
					if(connection != null) {
						connection.setIsFailed(new AtomicBoolean(true));
					}
					throw new SQLException(msg);
				}
				return new ScrayConnection(scrayURL, tAdapter);
			} else {
				return null;
			}
		} catch (URISyntaxException e) {
			if(connection != null) {
				connection.setIsFailed(new AtomicBoolean(true));
			}
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
