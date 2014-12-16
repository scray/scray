package scray.client.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class ScrayJdbcAccess {

	private Connection connect = null;
	private Statement statement = null;
	private ResultSet resultSet = null;

	public static void main(String[] args) {
		ScrayJdbcAccess jdbc = new ScrayJdbcAccess();
		try {
			jdbc.readDataBase();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void readDataBase() throws Exception {
		try {

			Class.forName("scray.client.jdbc.ScrayDriver");

			connect = DriverManager
					.getConnection("scray://localhost:8080/myDbSystem/myDbId/myTableId/myQuerySpace");

			statement = connect.createStatement();

			int count = 0;

			if (statement.execute("SELECT * FROM @myTableId")) {
				do {
					count++;
					ResultSet results = statement.getResultSet();
					System.out.println("Writing result set nr " + count + ".");
					writeResultSet(results);
				} while (statement.getMoreResults());

				System.out.println("Query finished.");

			}

		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}
	}

	private void writeResultSet(ResultSet resultSet) throws SQLException {
		int count = 0;
		while (resultSet.next()) {
			count++;
			ResultSetMetaData meta = resultSet.getMetaData();
			int size = meta.getColumnCount();
			System.out.println("Row " + count + " has " + size + " columns.");
			for (int i = 1; i <= size; i++) {
				String type = meta.getColumnClassName(i);
				Object value = resultSet.getObject(i);
				System.out
						.println("Column " + i + " (" + type + ") = " + value);
			}
		}
	}

	private void close() {
		close(resultSet);
		close(statement);
		close(connect);
	}

	private void close(AutoCloseable c) {
		try {
			if (c != null) {
				c.close();
			}
		} catch (Exception e) {
			// don't throw now as it might leave following closables in
			// undefined state
		}
	}

}
