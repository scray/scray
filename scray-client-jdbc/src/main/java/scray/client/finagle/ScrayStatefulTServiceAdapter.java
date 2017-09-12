//package scray.client.finagle;
//
//import java.sql.SQLException;
//import java.util.concurrent.locks.ReentrantReadWriteLock;
//
//import scray.service.qmodel.thriftjava.ScrayTQuery;
//import scray.service.qmodel.thriftjava.ScrayUUID;
//import scray.service.qservice.thriftjava.ScrayStatefulTService;
//import scray.service.qservice.thriftjava.ScrayTResultFrame;
//
//
//public class ScrayStatefulTServiceAdapter implements ScrayTServiceAdapter {
//
//	private ScrayStatefulTService.Client client = null;
//	private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
//	private String endpoint;
//
//	public ScrayStatefulTService.Client getClient() {
//		rwLock.readLock().lock();
//		try {
//			if(client == null) {
//				rwLock.readLock().unlock();
//				rwLock.writeLock().lock();
//				try {
//					if(client == null) {
////						client = null; Thrift.<ScrayStatefulTService.FutureIface>newIface(endpoint,
////								ScrayStatefulTService.AsyncClient.class); // FIXME stefan
//						
//					}
//				} finally {
//					rwLock.writeLock().unlock();
//					rwLock.readLock().lock();
//				}
//			}
//		} finally {
//			rwLock.readLock().unlock();
//		}
//		return client;
//	}
//
//	public ScrayStatefulTServiceAdapter(String endpoint) {
//		this.endpoint = endpoint;
//	}
//
//	public ScrayUUID query(ScrayTQuery query, int queryTimeout)
//			throws SQLException {
//		try {
//			ScrayUUID fuuid = getClient().query(query);
//			return fuuid; //Await.result(fuuid, Duration.fromSeconds(queryTimeout));
//		} catch (Exception e) {
//			throw new SQLException(e);
//		}
//	}
//
//	public ScrayTResultFrame getResults(ScrayUUID queryId, int queryTimeout)
//			throws SQLException {
//		try {
//			ScrayTResultFrame frame = getClient().getResults(queryId); // getResults(queryId);
//			//ScrayTResultFrame frame = Await.result(fframe,
//			//		Duration.fromSeconds(queryTimeout));
//			return frame;
//		} catch (Exception e) {
//			throw new SQLException(e);
//		}
//	}
//}
