package kv_interfaces;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import kvstore.exceptions.DupInsertException;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnAbortException;
import kvstore.exceptions.TxnException;
import main.Config;

public class PostgreSQLDB extends AbsNewSQL {
	private static boolean init = false;

	private static PostgreSQLDB instance = null;
	private static String table = null;

	// ======= singleton =======
	private PostgreSQLDB() {
		super(Config.get().PG_DB_HOSTS, Config.get().PG_PORTS);
	}

	public synchronized static PostgreSQLDB getInstance(String table) {
		if (instance == null) {
			instance = new PostgreSQLDB();
			instance.initTable(); // one time init
		}
		instance.table = table;
		return instance;
	}

	// ===== init =====
	// NOTE: should be call when creating the singleton
	// drop and recreate all tables for all benchmarks
	protected void initTable() {
		if (Config.get().BENCH_TYPE == Config.BenchType.TPCC) {
			tables = new String[] { "tpcc" };
//			return;
		}

		assert !init;
		init = true;

		String url = possible_urls[0];
		int port = possible_ports[0];
		String drop = "DROP TABLE IF EXISTS %s"; //
		String create = "CREATE TABLE IF NOT EXISTS %s (ID BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT)"; // for integer
																										// key
		// drop and create
		for (String tab : tables) {
			System.out.print("[INFO] Start to clear table[" + tab + "]...drop...");
			try {
				Connection conn = getConnection(url, port);
				// drop
				try {
					String sql1 = String.format(drop, tab);
					// for debug, comment this line
					conn.createStatement().execute(sql1);
				} catch (Exception e) {
					System.out.println(e.toString());
				}
				System.out.print("done...create...");

				// create
				try {
					String sql2 = String.format(create, tab);
					conn.createStatement().execute(sql2);
				} catch (SQLException e) {
					e.printStackTrace();
					System.exit(-1);
				}
				System.out.println("done");

				// check it is empty
				PreparedStatement st = conn.prepareStatement("SELECT * from " + tab);
				ResultSet rs = st.executeQuery();
				// should return nothing
				if (rs.next()) {
					System.err.println("[ERROR] drop-create table[" + tab + "] is not empty");
				} else {
					System.out.println("[INFO] table[" + tab + "] is empty");
				}
				conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
				System.exit(-1);
			}
		}
	}

	// ====== DB connection ==========
	@Override
	public Connection getConnection(String url, int port) throws SQLException {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
			System.exit(-1);
		}

		Config config = Config.get();
		String str_conn = String.format("jdbc:postgresql://%s:%d/%s", url, port, Config.get().PG_DATABASE_NAME);

		System.out.println(String.format("Trying go get conn: %s", str_conn));
		Connection conn = DriverManager.getConnection(
				str_conn,
				config.PG_USERNAME,
				config.PG_PASSWORD);

		return conn;
	}

	// ===== status managment ======
	public NewSQLConnection getConn4currentThread() {
		long threadId = Thread.currentThread().getId();
		if (!tid2conn.containsKey(threadId)) {
			PostgreSQLDBConnection conn = new PostgreSQLDBConnection(nextTxnId);
			tid2conn.put(threadId, conn);
			System.out.println(
					"[INFO] tid[" + threadId + "] starts a PostgreSQL connection at [" + conn.url + ":" + conn.port
							+ "]");
		}
		return tid2conn.get(threadId);
	}

	public void ReportServerDown() {
		long threadId = Thread.currentThread().getId();
		assert tid2conn.containsKey(threadId);
		tid2conn.remove(threadId);
	}

	// ===============inner class================
	class PostgreSQLDBConnection extends NewSQLConnection {
		public PostgreSQLDBConnection(AtomicLong tid) {
			super(tid);
		}

		@Override
		public boolean insert(Object txn, String table, String keyStr, String value) throws KvException, TxnException {
			throw new UnsupportedOperationException("Unimplemented method 'insert'");
		}

		public boolean delete(Object txn, String table, String keyStr) throws KvException, TxnException {
			assert conn != null;
			assert (Long) txn == currTxnId && currTxnId != -1;
			num_op++;

			// tricky part
			Object key = keyStr2Long(keyStr);

			PreparedStatement stmt = null;
			try {
				stmt = conn.prepareStatement("DELETE FROM " + table + " WHERE ID = ?");
				if (key instanceof Long)
					stmt.setLong(1, (Long) key);
				else
					stmt.setString(1, (String) key);
				int updatedRows = stmt.executeUpdate();
				if (updatedRows == 0) {
					return false;
				}
			} catch (SQLException e) {
				String errMsg = e.getMessage();
				if (errMsg.contains("could not serialize access due to concurrent update")) {
					throw new TxnAbortException(e.getMessage());
				}
				throw new TxnException(e.getMessage());
			} finally {
				try {
					if (stmt != null)
						stmt.close();
				} catch (Exception e) {
				}
				;
			}
			return true;
		}

		public String get(Object txn, String table, String keyStr) throws KvException, TxnException {
			throw new UnsupportedOperationException("Unimplemented method 'get'");
		}

		public boolean set(Object txn, String table, String keyStr, String value) throws KvException, TxnException {
			return this.upsert(txn, table, keyStr, value);
		}

		private boolean upsert(Object txn, String table, String keyStr, String value) throws KvException, TxnException {
			assert conn != null;
			assert (Long) txn == currTxnId && currTxnId != -1;
			num_op++;

			// tricky part
			Object key = keyStr2Long(keyStr);
			Statement stmt = null;
			try {
				stmt = conn.createStatement();
				String sqlstmt = String.format(PGSQLTemplates.getUpsertTemplate(key),
						table, key,
						value, value);
				stmt.execute(sqlstmt);
			} catch (SQLException e) {
				String errMsg = e.getMessage();
				if (errMsg.contains("already exists.")) {
					throw new DupInsertException(errMsg);
				}
				if (errMsg.contains("could not serialize access due to concurrent update")) {
					throw new TxnAbortException(e.getMessage());
				}
				throw new TxnException(e.getMessage());
			} finally {
				try {
					if (stmt != null)
						stmt.close();
				} catch (Exception e) {
				}
				;
			}
			return true;
		}

		public Map<Long, String> range(Object txn, String table, String key1Str, String key2Str)
				throws KvException, TxnException {
			throw new UnsupportedOperationException("Unimplemented method 'range'");
		}

		public Object begin() throws KvException, TxnException {
			assert conn != null;
			assert currTxnId == -1; // last transaction is finished
			currTxnId = nextTxnId.getAndIncrement();
			// try {
			// writeBuffer = conn.createStatement();
			// } catch (SQLException e) {
			// e.printStackTrace();
			// System.exit(-1);
			// }
			num_op++;
			return currTxnId;
		}

		public boolean commit(Object txn) throws KvException, TxnException {
			assert conn != null;
			assert (Long) txn == currTxnId && currTxnId != -1;
			num_op++;

			try {
				conn.commit();
				currTxnId = -1;
			} catch (SQLException e) {
				throw new TxnException(e.getMessage());
			}
			return true;
		}

		public boolean abort(Object txn) throws KvException, TxnException {
			assert conn != null;
			assert (Long) txn == currTxnId && currTxnId != -1;
			num_op++;

			try {
				conn.rollback();
				currTxnId = -1;
			} catch (SQLException e) {
				throw new TxnException(e.getMessage());
			}
			return true;
		}

		public boolean rollback(Object txn) {
			assert currTxnId == (Long) txn && currTxnId != -1;
			num_op++;
			currTxnId = -1;
			try {
				conn.rollback();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return true;
		}
	}
}
