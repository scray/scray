package scray.client.finagle;

import scray.service.qservice.thriftjava.ScrayStatelessTService;

import com.twitter.finagle.Thrift;

public class FinagleThriftConnection {

	private ScrayStatelessTService.FutureIface client;

	public FinagleThriftConnection(String endpoint) {
		client = Thrift.newIface(endpoint, ScrayStatelessTService.FutureIface.class);
	}

	public ScrayStatelessTService.FutureIface getScrayTService() {
		return client;
	}

}
