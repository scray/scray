package scray.client.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScrayJdbcAccess {

	private static Logger logger = LoggerFactory
			.getLogger(ScrayJdbcAccess.class);

	private Connection connect = null;
	private Statement statement = null;
	private ResultSet resultSet = null;
	private long totalcount = 0L;

	/* defaults for options */
	private int FETCHSIZE = 50;
	private int TIMEOUT = 10;
	private int RESULTSETS = -1;
	private String URL = "jdbc:scray:stateful://s030l0331:18191/cassandra/SILNP/SIL";
	private String STATEMENT = "SELECT * FROM BISMTOlsWorkflowElement WHERE (creationTime > 1L) LIMIT 10001";
	private boolean DOTS = false;

	public static void main(String[] args) {
		ScrayJdbcAccess jdbc = new ScrayJdbcAccess();
		if (ScrayJdbcAccessParser.parseCLIOptions(jdbc, args)) {
			try {
				jdbc.readDataBase();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public ScrayJdbcAccess() {
		try {
			Class.forName("scray.client.jdbc.ScrayDriver");
		} catch (Exception e) {
			logger.error("Could not initialize driver", e);
		}
	}

	public void readDataBase() throws Exception {
		try {
			connect = DriverManager.getConnection(URL);

			statement = connect.createStatement();

			statement.setQueryTimeout(TIMEOUT);
			statement.setFetchSize(FETCHSIZE);

			int resultSets = RESULTSETS;

			int count = 0;
			long aggTime = 0;
			long snap = System.currentTimeMillis();
			if (statement.execute(STATEMENT)) {
				do {
					count++;
					ResultSet results = statement.getResultSet();
					long nextTime = System.currentTimeMillis() - snap;
					aggTime += nextTime;

					if (!DOTS) {
						System.out.println();
						System.out
								.println("====================================================================");
						System.out
								.println("====================================================================");
						System.out
								.println("====================================================================");

						System.out.println("Result set nr " + count
								+ " loaded in " + nextTime + " ms.");

						System.out
								.println("====================================================================");
						System.out
								.println("====================================================================");
						System.out
								.println("====================================================================");
					}
					
					writeResultSet(results);
					snap = System.currentTimeMillis();
					
				} while (statement.getMoreResults()&& count != (resultSets - 1));
				
				System.out.println();
				
				System.out
						.println("====================================================================");
				System.out
						.println("====================================================================");
				System.out
						.println("====================================================================");

				System.out.println("Finished - fetched " + totalcount
						+ " result(s) in " + count
						+ " result set(s) with pagesize of " + FETCHSIZE
						+ " in " + aggTime + " ms.");

				System.out
						.println("====================================================================");
				System.out
						.println("====================================================================");
				System.out
						.println("====================================================================");

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
			totalcount++;
			if (!DOTS) {
				ResultSetMetaData meta = resultSet.getMetaData();
				int size = meta.getColumnCount();
				System.out.println();
				System.out.println("Row " + count + " has " + size
						+ " columns.");
				for (int i = 1; i <= size; i++) {
					String type = meta.getColumnClassName(i);
					Object value = resultSet.getObject(i);
					System.out.println("Column " + i + "  '"
							+ meta.getColumnName(i) + "'  (" + type + ") = "
							+ value);
				}
			} else if (totalcount % 100L == 0) {
				System.out.print(".");
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

	public void setFETCHSIZE(int fETCHSIZE) {
		FETCHSIZE = fETCHSIZE;
	}

	public void setTIMEOUT(int tIMEOUT) {
		TIMEOUT = tIMEOUT;
	}

	public void setRESULTSETS(int rESULTSETS) {
		RESULTSETS = rESULTSETS;
	}

	public void setURL(String uRL) {
		URL = uRL;
	}

	public void setDOTS(boolean dOTS) {
		DOTS = dOTS;
	}

	public void setSTATEMENT(String statement) {
		STATEMENT = statement;
	}
}
