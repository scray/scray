package scray.client.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class ScrayJdbcAccess {

	private static Logger logger = LoggerFactory
			.getLogger(ScrayJdbcAccess.class);

	private static final ScrayJdbcAccess scrayJdbcAccess = new ScrayJdbcAccess();

	/* request options */
	class AccessRequestOptions {
		/* defaults */
		int fetchsize = 50;
		int timeout = 10;
		int resultsets = -1;		
		String url = "jdbc:scray:stateful://s030l0331,s030l0334:18181/cassandra/SILNP/SIL";
		String query = "SELECT * FROM BISMTOlsWorkflowElement WHERE (creationTime > 1L) AND (creationTime < 4000000000000L) LIMIT 10000";
		boolean dots = true;
		boolean stress = true;
		int stressCount = 200;
	}

	/* query state */
	class QueryExecutionState {
		Connection connect = null;
		Statement statement = null;
		ResultSet resultSet = null;
		long totalcount = 0L;
	}

	public static void main(String[] args) {
		AccessRequestOptions options = scrayJdbcAccess.new AccessRequestOptions();
		if (ScrayJdbcAccessParser.parseCLIOptions(options, args)) {
			try {
				if (options.stress) {
					scrayJdbcAccess.stress(options);
				} else {
					scrayJdbcAccess.readDataBase(options,
							scrayJdbcAccess.new QueryExecutionState());
				}
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

	public void stress(AccessRequestOptions opts) throws Exception {
		QueryThreadPoolExecutor qtpe;
		if (opts.stressCount <= 0) {
			qtpe = new QueryThreadPoolExecutor(100);
			int count = 0;
			while (true) {
				if (qtpe.workQueue.remainingCapacity() > 0) {
					qtpe.runTask(new QueryWorker(opts, ++count));
				}
				Thread.sleep(1000);
			}
		} else {
			qtpe = new QueryThreadPoolExecutor(opts.stressCount);
			for (int i = 0; i < opts.stressCount; i++) {
				qtpe.runTask(new QueryWorker(opts, i));
			}
		}
		qtpe.shutDown();
		System.out.println("Finished! :)");
	}

	class QueryThreadPoolExecutor {
		// Threads to be kept idle all time
		int corePoolSize = 10;

		// Maximum Threads allowed in Pool
		int maxPoolSize = 10;

		// Keep alive time for waiting threads for jobs(Runnable)
		long keepAliveTime = 10;

		// This is the one who manages and start the work
		ThreadPoolExecutor threadPool = null;

		// Working queue for jobs (Runnable). We add them finally here
		ArrayBlockingQueue<Runnable> workQueue;

		public QueryThreadPoolExecutor(int nr) {
			this.workQueue = new ArrayBlockingQueue<Runnable>(nr);
			threadPool = new ThreadPoolExecutor(corePoolSize, maxPoolSize,
					keepAliveTime, TimeUnit.SECONDS, workQueue);
		}

		/**
		 * Here we add our jobs to working queue
		 *
		 * @param task
		 *            a Runnable task
		 */
		public void runTask(Runnable task) {
			threadPool.execute(task);
			System.out.println("Tasks in workQueue.." + workQueue.size());
		}

		/**
		 * Shutdown the Threadpool if it’s finished
		 */
		public void shutDown() {
			threadPool.shutdown();
		}

	}

	/**
	 * This is the one who does the work This one is static for accessing from
	 * main class
	 */
	private static class QueryWorker implements Runnable {
		
		// so we can see which job is running
		private int jobNr;
		private AccessRequestOptions opts;

		/**
		 * @param opts
		 *            some query opts
		 * @param jobNr
		 *            number for displaying
		 */
		public QueryWorker(AccessRequestOptions opts, int jobNr) {
			this.opts = opts;
			this.jobNr = jobNr;
		}

		@Override
		public void run() {
			try {
				System.out.println("Going to run query nr " + jobNr);
				scrayJdbcAccess.readDataBase(opts,
						scrayJdbcAccess.new QueryExecutionState());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void readDataBase(AccessRequestOptions opts,
			QueryExecutionState state) throws Exception {
		try {
			state.connect = DriverManager.getConnection(opts.url);
			state.statement = state.connect.createStatement();
			state.statement.setQueryTimeout(opts.timeout);
			state.statement.setFetchSize(opts.fetchsize);

			int count = 0;
			long aggTime = 0;
			long snap = System.currentTimeMillis();

			if (state.statement.execute(opts.query)) {
				do {
					count++;
					ResultSet results = state.statement.getResultSet();
					long nextTime = System.currentTimeMillis() - snap;
					aggTime += nextTime;

					if (!opts.dots) {
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

					writeResultSet(results, opts.dots, state);
					snap = System.currentTimeMillis();

				} while (state.statement.getMoreResults()
						&& count != (opts.resultsets - 1));

				System.out.println();

				System.out
						.println("====================================================================");
				System.out
						.println("====================================================================");
				System.out
						.println("====================================================================");

				System.out.println("Finished - fetched " + state.totalcount
						+ " result(s) in " + count
						+ " result set(s) with pagesize of " + opts.fetchsize
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
			close(state);
		}
	}

	private void writeResultSet(ResultSet resultSet, boolean dots,
			QueryExecutionState state) throws SQLException {
		int count = 0;
		while (resultSet.next()) {
			count++;
			state.totalcount++;
			if (!dots) {
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
			} else if (state.totalcount % 100L == 0) {
				System.out.print(".");
			}
		}
	}

	private void close(QueryExecutionState state) {
		close(state.resultSet);
		close(state.statement);
	}

	private void close(AutoCloseable c) {
		try {
			if (c != null) {
				c.close();
			}
		} catch (Exception e) {
			System.out.println(e);
			// don't throw now as it might leave following closables in
			// undefined state
		}
	}

}
