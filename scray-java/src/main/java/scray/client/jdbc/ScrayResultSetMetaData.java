package scray.client.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class ScrayResultSetMetaData implements ResultSetMetaData {

	// corresponding result set
	private ScrayResultSet resultSet;

	public ScrayResultSetMetaData(ScrayResultSet resultSet) {
		this.resultSet = resultSet;
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		return new String();
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		Object value = resultSet.getObject(column);
		return value.getClass().getName();
	}

	@Override
	public int getColumnCount() throws SQLException {
		return resultSet.getColumnList().size();
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return resultSet.getColumnList().get(column - 1).getColumnInfo()
				.getName();
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getScale(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		resultSet.getStatement().getConnection().getSchema();
		return null;
	}

	@Override
	public String getTableName(int column) throws SQLException {
		ScrayStatement stm = (ScrayStatement) resultSet.getStatement();
		return stm.getTableId();
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int isNullable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

}
