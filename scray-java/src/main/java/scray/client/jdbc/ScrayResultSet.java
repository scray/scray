package scray.client.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scray.common.serialization.pool.KryoJavaPoolSerialization;
import scray.service.qmodel.thriftjava.ScrayTColumn;
import scray.service.qmodel.thriftjava.ScrayTRow;
import scray.service.qservice.thriftjava.ScrayTResultFrame;

public class ScrayResultSet implements java.sql.ResultSet {

	class CachePosition {
		int row;
		String column;

		public CachePosition(int row, String column) {
			this.row = row;
			this.column = column;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			CachePosition other = (CachePosition) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (column == null) {
				if (other.column != null)
					return false;
			} else if (!column.equals(other.column))
				return false;
			if (row != other.row)
				return false;
			return true;
		}

		private ScrayResultSet getOuterType() {
			return ScrayResultSet.this;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result
					+ ((column == null) ? 0 : column.hashCode());
			result = prime * result + row;
			return result;
		}

		@Override
		public String toString() {
			return "CachePosition [row=" + row + ", column=" + column + "]";
		}
	}

	private boolean closed = false;

	// the corresponding statement
	private ScrayStatement statement;

	// row list extracted from frame
	private List<ScrayTRow> rows;

	// cursor marking the current iterator position
	private int cursor = 0;

	// column list of the row at cursor position
	private List<ScrayTColumn> columnList;

	// column map caching deserialized values of all rows
	private Map<CachePosition, Object> valueCache = new HashMap<CachePosition, Object>();
	private int fetchSize;

	private int fetchDirection;

	private boolean isLastResultSet;

	public ScrayResultSet(ScrayTResultFrame frame, int fetchSize,
			int fetchDirection, ScrayStatement statement) {
		this.fetchSize = fetchSize;
		this.fetchDirection = fetchDirection;
		rows = frame.getRows();
		this.statement = statement;
		this.isLastResultSet = !rows.get(rows.size() - 1).isSetColumns();
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		if (row < 0) {
			if ((row * -1) > rows.size()) {
				cursor = 0;
				return false;
			} else {
				changeCursor(rows.size() + row);
				return true;
			}
		} else if (row > 0) {
			if (row > rows.size()) {
				cursor = getLastIndex();
				return false;
			} else {
				changeCursor(row);
				return true;
			}
		} else {
			cursor = 0;
			return false;
		}
	}

	@Override
	public void afterLast() throws SQLException {
		absolute(getLastIndex());
	}

	@Override
	public void beforeFirst() throws SQLException {
		absolute(0);
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	private void changeCursor(int newCursor) throws SQLException {
		cursor = newCursor;
		ScrayTRow row = rows.get(cursor - 1);
		if (row.isSetColumns()) {
			columnList = row.getColumns();
			for (ScrayTColumn tcol : row.getColumns()) {
				CachePosition pos = new CachePosition(cursor, tcol
						.getColumnInfo().getName());
				if (!valueCache.containsKey(pos)) {
					valueCache.put(pos, decode(tcol.getValue()));
				}
			}
		}
	}

	private void checkConstraints() throws SQLException {
		if (isBeforeFirst() || isAfterLast()) {
			throw new SQLException("Cursor out of bounds.");
		}
		if (isClosed()) {
			throw new SQLException("ResultSet is closed.");
		}
	}

	@Override
	public void clearWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void close() throws SQLException {
		rows = null;
		columnList = null;
		valueCache = null;

		closed = true;
	}

	private Object decode(ByteBuffer bytes) throws SQLException {
		try {
			byte[] bytesArr = new byte[bytes.remaining()];
			bytes.get(bytesArr);
			return KryoJavaPoolSerialization.getInstance().chill
					.fromBytes(bytesArr);
		} catch (Exception ex) {
			throw new SQLException("Serialization error.", ex);
		}
	}

	private Object decode(int columnIndex) throws SQLException {
		// column index starts at 1
		if (columnIndex < 1 || columnIndex > columnList.size())
			throw new SQLException("Column index out of bounds.");
		return decode(columnList.get(columnIndex - 1).getColumnInfo().getName());
	}

	private <T> T decode(int columnIndex, Class<T> klass) throws SQLException {
		// column index starts at 1
		if (columnIndex < 1 || columnIndex > columnList.size())
			throw new SQLException("Column index out of bounds.");
		return decode(
				columnList.get(columnIndex - 1).getColumnInfo().getName(),
				klass);
	}

	private Object decode(String columnLabel) throws SQLException {
		CachePosition pos = new CachePosition(cursor, columnLabel);
		if (!valueCache.containsKey(pos))
			throw new SQLException("Unknown column label '" + columnLabel
					+ "' in row " + cursor + ".");
		return valueCache.get(pos);
	}

	private <T> T decode(String columnLabel, Class<T> klass)
			throws SQLException {
		CachePosition pos = new CachePosition(cursor, columnLabel);
		if (!valueCache.containsKey(pos))
			throw new SQLException("Unknown column label '" + columnLabel
					+ "' in row " + cursor + ".");
		return klass.cast(valueCache.get(pos));
	}

	@Override
	public void deleteRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		int found = columnList.indexOf(columnLabel);
		if (found == -1) {
			throw new SQLException("Unknown column label '" + columnLabel
					+ "' in row " + cursor + ".");
		}
		return found;
	}

	@Override
	public boolean first() throws SQLException {
		return absolute(1);
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		checkConstraints();
		return decode(columnIndex, BigDecimal.class);
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		checkConstraints();
		return decode(columnLabel, BigDecimal.class);
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		checkConstraints();
		return decode(columnIndex, Boolean.class);
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		checkConstraints();
		return decode(columnLabel, Boolean.class);
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		checkConstraints();
		return decode(columnIndex, byte[].class);
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		checkConstraints();
		return decode(columnLabel, byte[].class);
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	protected List<ScrayTColumn> getColumnList() {
		return columnList;
	}

	@Override
	public int getConcurrency() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getCursorName() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		checkConstraints();
		return decode(columnIndex, Double.class);
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		checkConstraints();
		return decode(columnLabel, Double.class);
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
	public float getFloat(int columnIndex) throws SQLException {
		checkConstraints();
		return decode(columnIndex, Float.class);
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		checkConstraints();
		return decode(columnLabel, Float.class);
	}

	@Override
	public int getHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		checkConstraints();
		return decode(columnIndex, Integer.class);
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		checkConstraints();
		return decode(columnLabel, Integer.class);
	}

	private int getLastIndex() {
		if (isLastResultSet) {
			return rows.size() - 1;
		} else {
			return rows.size();
		}
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		checkConstraints();
		return decode(columnIndex, Long.class);
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		checkConstraints();
		return decode(columnLabel, Long.class);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		checkConstraints();
		return new ScrayResultSetMetaData(this);
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		checkConstraints();
		return decode(columnIndex);
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		checkConstraints();
		return decode(columnIndex, type);
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		checkConstraints();
		return decode(columnLabel);
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type)
			throws SQLException {
		checkConstraints();
		return decode(columnLabel, type);
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getRow() throws SQLException {
		return cursor;
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Statement getStatement() throws SQLException {
		return statement;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		checkConstraints();
		return decode(columnIndex, String.class);
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		checkConstraints();
		return decode(columnLabel, String.class);
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getType() throws SQLException {
		return ResultSet.TYPE_SCROLL_INSENSITIVE;
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void insertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		return cursor > getLastIndex();
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		return cursor < 1;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return closed;
	}

	@Override
	public boolean isFirst() throws SQLException {
		return cursor == 1;
	}

	@Override
	public boolean isLast() throws SQLException {
		return cursor == rows.size();
	}

	public boolean isLastResultSet() {
		return isLastResultSet;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean last() throws SQLException {
		return absolute(-1);
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean next() throws SQLException {
		if (cursor < getLastIndex()) {
			changeCursor(cursor + 1);
			return true;
		} else if (cursor == getLastIndex()) {
			// move to "after last"
			cursor++;
			return false;
		} else {
			// stay on "after last"
			return false;
		}
	}

	@Override
	public boolean previous() throws SQLException {
		if (cursor > 1) {
			changeCursor(cursor - 1);
			return true;
		} else {
			// cursor is on "beforeFirst" position
			cursor = 0;
			return false;
		}
	}

	@Override
	public void refreshRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		return absolute(cursor + rows);
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean rowInserted() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setFetchDirection(int fetchDirection) throws SQLException {
		this.fetchDirection = fetchDirection;
	}

	@Override
	public void setFetchSize(int fetchSize) throws SQLException {
		this.fetchSize = fetchSize;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNString(int columnIndex, String nString)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNString(String columnLabel, String nString)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean wasNull() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

}
