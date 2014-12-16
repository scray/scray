package scray.client.finagle;

import scray.service.qservice.thriftjava.ScrayTService;

import com.twitter.finagle.Thrift;

public class FinagleThriftConnection {

	private ScrayTService.FutureIface client;

	public FinagleThriftConnection(String endpoint) {
		client = Thrift.newIface(endpoint, ScrayTService.FutureIface.class);
	}

	public ScrayTService.FutureIface getScrayTService() {
		return client;
	}

}
