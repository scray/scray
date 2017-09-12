//package scray.client.finagle;
//
//import java.sql.SQLException;
//
//import com.twitter.finagle.Service;
//import com.twitter.finagle.Thrift;
//import com.twitter.finagle.builder.ClientBuilder;
//import com.twitter.finagle.thrift.ThriftClientRequest;
//import com.twitter.util.Await;
//import com.twitter.util.Duration;
//import com.twitter.util.Future;
//
//import scray.service.qmodel.thriftjava.ScrayTQuery;
//import scray.service.qmodel.thriftjava.ScrayUUID;
//import scray.service.qservice.thriftjava.ScrayCombinedStatefulTService;
//import scray.service.qservice.thriftjava.ScrayCombinedStatefulTService.Client;
//
//import com.twitter.finagle.builder.ClientBuilder;
//import scray.service.qservice.thriftjava.ScrayTResultFrame;
//
//class ScrayCombinedStatefulTServiceAdapter implements ScrayTServiceAdapter {
//
//	private ScrayCombinedStatefulTService.AsyncClient client;
//	private String endpoint;
//
//	public ScrayCombinedStatefulTService.Client getClient() {
//		// lazy init
//		if (client == null) {
//			
//			   Service<ThriftClientRequest, byte[]> ff = ClientBuilder.safeBuild(
//					     ClientBuilder.get()
//					       .stack(Thrift.client())
//					       .hosts("localhost:10000,localhost:10001,localhost:10003")
//					       .hostConnectionLimit(1));
//		
//			
//			Client clientService = new ScrayCombinedStatefulTService.Client(ff);
//		}
////					new ClientBuilder()
////				      .hosts()
////				      .stack(Thrift.client)
////				      .hostConnectionLimit(1)
////				.build()
////			client = scray.service.qservice.thriftjava.ScrayCombinedStatefulTService.Client;
////			client = ScrayCombinedStatefulTService.C
//			//Thrift.newIface(endpoint,
////					ScrayCombinedStatefulTService.FutureIface.class);
//		}
//		return client;
//	}
//
//	public ScrayCombinedStatefulTServiceAdapter(String endpoint) {
//		this.endpoint = endpoint;
//	}
//
//	public ScrayUUID query(ScrayTQuery query, int queryTimeout)
//			throws SQLException {
//		try {
//			ScrayUUID fuuid = getClient().query(query);
//			return Await.result(fuuid, Duration.fromSeconds(queryTimeout));
//		} catch (Exception e) {
//			throw new SQLException(e);
//		}
//	}
//
//	public ScrayTResultFrame getResults(ScrayUUID queryId, int queryTimeout)
//			throws SQLException {
//		try {
//			Future<ScrayTResultFrame> fframe = getClient().getResults(queryId);
//			ScrayTResultFrame frame = Await.result(fframe,
//					Duration.fromSeconds(queryTimeout));
//			return frame;
//		} catch (Exception e) {
//			throw new SQLException(e);
//		}
//	}
//}
