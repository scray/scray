package scray.client.jdbc;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import scray.service.qmodel.thriftjava.ScrayTColumnInfo;
import scray.service.qmodel.thriftjava.ScrayTQuery;
import scray.service.qmodel.thriftjava.ScrayTQueryInfo;
import scray.service.qmodel.thriftjava.ScrayTTableInfo;
import scray.service.qmodel.thriftjava.ScrayUUID;
import scray.service.qservice.thriftjava.ScrayTResultFrame;

import com.twitter.scrooge.Option;
import com.twitter.util.Await;
import com.twitter.util.Duration;
import com.twitter.util.Future;

public class ScrayStatement implements java.sql.Statement {

	public static final int DEFAULT_FETCH_SIZE = 50;
	public static final int DEFAULT_FETCH_DIRECTION = ResultSet.FETCH_FORWARD;
	public static final int MAX_ROWS_MAX = 5000;
	public static final int DEFAULT_MAX_ROWS = 1000;
	public static final int DEFAULT_QUERY_TIMEOUT = 10;

	private int fetchDirection = DEFAULT_FETCH_DIRECTION;
	private int fetchSize = DEFAULT_FETCH_SIZE;
	private int maxrows = DEFAULT_MAX_ROWS;
	private int queryTimeout = DEFAULT_QUERY_TIMEOUT;

	private boolean closed = false;
	private ScrayConnection connection = null;

	private ScrayTQuery rawTQuery = null;
	private String tableId = null;
	private ScrayUUID queryId = null;
	private ScrayTResultFrame currFrame = null;
	private ScrayResultSet currResults = null;

	public ScrayStatement(ScrayConnection connection) {
		this.connection = connection;
	}

	private void advanceQuery() throws SQLException {
		try {
			currFrame = syncFetch(queryId);
			currResults = new ScrayResultSet(currFrame, fetchSize,
					fetchDirection, this);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	private void checkConstraints() throws SQLException {
		if (isClosed()) {
			throw new SQLException("Statement is closed.");
		}
	}

	@Override
	public void close() throws SQLException {
		connection = null;
		currFrame = null;
		currResults = null;
		queryId = null;
		rawTQuery = null;

		closed = true;
	}

	private ScrayTQuery createScrayTQuery(String sql) throws SQLException {

		ScrayURL url = connection.getScrayURL();

		// extract table id and enrich query expression with scray specific '@'
		// character prefix

		int idx = sql.toLowerCase().indexOf("from") + 4;

		if (idx == -1) {
			throw new SQLException("FROM-part missing in query.");
		}

		while (sql.charAt(idx) == ' ' && idx < sql.length() - 1) {
			idx++;
		}

		if (idx == sql.length() - 1) {
			throw new SQLException("Table identifier missing in query.");
		}

		String enrichedSql = sql.substring(0, idx) + "@" + sql.substring(idx);

		StringBuffer sbuf = new StringBuffer();

		while (sql.charAt(idx) != ' ' && idx < sql.length() - 1) {
			sbuf.append(sql.charAt(idx));
			idx++;
		}

		sbuf.append(sql.charAt(idx));

		tableId = sbuf.toString();

		// create scray query

		// we don't use protocol level column spec here
		List<ScrayTColumnInfo> clist = new LinkedList<ScrayTColumnInfo>();

		ScrayTTableInfo tinfo = new ScrayTTableInfo(url.getDbSystem(),
				url.getDbId(), tableId);

		ScrayTQueryInfo qinfo = new ScrayTQueryInfo(Option.<ScrayUUID> none(),
				url.getQuerySpace(), tinfo, clist,
				Option.make(true, fetchSize), Option.<Long> none());

		ScrayTQuery query = new ScrayTQuery(qinfo,
				new HashMap<String, ByteBuffer>(), enrichedSql);

		return query;
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		checkConstraints();
		initializeQuery(sql);
		advanceQuery();
		return true;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		checkConstraints();
		initializeQuery(sql);
		advanceQuery();
		return currResults;
	}

	@Override
	public Connection getConnection() throws SQLException {
		return connection;
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return fetchDirection;
	}

	@Override
	public int getFetchSize() throws SQLException {
		return fetchSize;
	}

	@Override
	public int getMaxRows() throws SQLException {
		return maxrows;
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		checkConstraints();
		if (currResults.isLastResultSet()) {
			return false;
		} else {
			advanceQuery();
			return true;
		}
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return queryTimeout;
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		checkConstraints();
		return currResults;
	}

	private void initializeQuery(String sql) throws SQLException {
		try {
			rawTQuery = createScrayTQuery(sql);
			queryId = syncSubmit(rawTQuery);
		} catch (Exception e) {
			throw new SQLException(e);
		}
	}

	@Override
	public boolean isClosed() throws SQLException {
		return closed;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		fetchDirection = direction;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		fetchSize = rows;
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		if (max > MAX_ROWS_MAX) {
			maxrows = MAX_ROWS_MAX;
		} else {
			maxrows = max;
		}
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		queryTimeout = seconds;
	}

	private ScrayTResultFrame syncFetch(ScrayUUID uuid) throws Exception {
		Future<ScrayTResultFrame> fframe = connection.getThriftConnection()
				.getScrayTService().getResults(uuid);
		ScrayTResultFrame frame = Await.result(fframe, new Duration(
				queryTimeout * 1000000000L));
		return frame;
	}

	private ScrayUUID syncSubmit(ScrayTQuery query) throws Exception {
		Future<ScrayUUID> fuuid = connection.getThriftConnection()
				.getScrayTService().query(rawTQuery);
		return Await.result(fuuid, new Duration(queryTimeout * 1000000000L));
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void cancel() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void clearWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getUpdateCount() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetType() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public String getTableId() {
		return tableId;
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void clearBatch() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, String[] columnNames)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isPoolable() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

}
