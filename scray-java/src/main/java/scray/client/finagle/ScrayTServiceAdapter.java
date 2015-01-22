package scray.client.finagle;

import java.sql.SQLException;

import scray.service.qmodel.thriftjava.ScrayTQuery;
import scray.service.qmodel.thriftjava.ScrayUUID;
import scray.service.qservice.thriftjava.ScrayTResultFrame;

public interface ScrayTServiceAdapter {

	public ScrayUUID query(ScrayTQuery query, int queryTimeout)
			throws SQLException;

	public ScrayTResultFrame getResults(ScrayUUID queryId, int queryTimeout)
			throws SQLException;
		
}
