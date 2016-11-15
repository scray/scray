package scray.client.finagle;

import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import scray.service.qmodel.thriftjava.ScrayTQuery;
import scray.service.qmodel.thriftjava.ScrayUUID;
import scray.service.qservice.thriftjava.ScrayStatefulTService;
import scray.service.qservice.thriftjava.ScrayTResultFrame;

import com.twitter.finagle.Service;
import com.twitter.finagle.Thrift;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.thrift.ThriftClientFramedCodec;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;

public class ScrayStatefulTServiceAdapter implements ScrayTServiceAdapter {

	private ScrayStatefulTService.FutureIface client = null;
	private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
	private String endpoint;

	public ScrayStatefulTService.FutureIface getClient() {
		rwLock.readLock().lock();
		try {
			if(client == null) {
				rwLock.readLock().unlock();
				rwLock.writeLock().lock();
				try {
					if(client == null) {
						client = Thrift.<ScrayStatefulTService.FutureIface>newIface(endpoint,
								ScrayStatefulTService.FutureIface.class);
						
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
			Future<ScrayTResultFrame> fframe = getClient().getResults(queryId);
			ScrayTResultFrame frame = Await.result(fframe,
					Duration.fromSeconds(queryTimeout));
			return frame;
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}
}
