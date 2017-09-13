package scray.client.finagle;

import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.twitter.finagle.Thrift;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;

import scray.service.qmodel.thriftjava.ScrayTQuery;
import scray.service.qmodel.thriftjava.ScrayUUID;
import scray.service.qservice.thriftjava.ScrayStatefulTService;
import scray.service.qservice.thriftjava.ScrayStatefulTService.ServiceIface;
import scray.service.qservice.thriftjava.ScrayTResultFrame;


public class ScrayStatefulTServiceAdapter implements ScrayTServiceAdapter {

	private ServiceIface client = null;
	private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
	private String endpoint;

	public ServiceIface getClient() {
		rwLock.readLock().lock();
		try {
			if(client == null) {
				rwLock.readLock().unlock();
				rwLock.writeLock().lock();
				try {
					if(client == null) {
						client = Thrift.client().<ScrayStatefulTService.ServiceIface>newIface(endpoint,
								ScrayStatefulTService.ServiceIface.class);
					}
				} finally {
					rwLock.writeLock().unlock();
					rwLock.readLock().lock();
				}
			}
		} finally {
			rwLock.readLock().unlock();
		}
		return client;
	}

	public ScrayStatefulTServiceAdapter(String endpoint) {
		this.endpoint = endpoint;
	}

	public ScrayUUID query(ScrayTQuery query, int queryTimeout)
			throws SQLException {
		try {
			Future<ScrayUUID> fuuid = getClient().query(query);
			return Await.result(fuuid, Duration.fromSeconds(queryTimeout));
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	public ScrayTResultFrame getResults(ScrayUUID queryId, int queryTimeout)
			throws SQLException {
		try {
			Future<ScrayTResultFrame> fframe = getClient().getResults(queryId); // getResults(queryId);
			ScrayTResultFrame frame = Await.result(fframe, Duration.fromSeconds(queryTimeout));
			return frame;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
}
