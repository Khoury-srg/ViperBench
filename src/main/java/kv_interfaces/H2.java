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


public class H2 extends AbsNewSQL {
    private static boolean init = false;

    private static H2 instance = null;

    private H2() {
        super(Config.get().H2_DB_URLS, Config.get().H2_PORTS);
    }

    public synchronized static H2 getInstance() {
        if (instance == null) {
            instance = new H2();
            instance = new H2();
            instance.initTable(); // one time init
        }
        return instance;
    }

    // ===== init =====

    // NOTE: should be call when creating the singleton
    // drop and recreate all tables for all benchmarks
    protected void initTable() {
        assert !init;
        init = true;

        if(Config.get().BENCH_TYPE == Config.BenchType.TPCC) {
            tables = new String[]{"tpcc"};
            return;
        }

        String url = possible_urls[0];
        int port = possible_ports[0];
        String drop = "DROP TABLE IF EXISTS %s";
        String create = null;
        if(Config.get().KEY_TYPE == 0)
            // CREATE TABLE IF NOT EXISTS %s (ID VARCHAR PRIMARY KEY, value VARCHAR(255));
            create = "CREATE TABLE IF NOT EXISTS %s (ID VARCHAR(255) PRIMARY KEY, val VARCHAR(3000));"; // for string
            // key
        else
            create = "CREATE TABLE IF NOT EXISTS %s (ID BIGINT PRIMARY KEY, val VARCHAR(3000))"; // for integer key


//         drop and create
        for (String tab : tables) {
            System.out.print("[INFO] Start to clear table[" + tab + "]...drop...");
            try {
                Connection conn = getConnection(url, port);
                // drop
                try {
                    String sql1 = String.format(drop, tab);
                    conn.createStatement().execute(sql1);
                } catch (SQLException e) {
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
//        try {
//            Class.forName("com.mysql.cj.jdbc.Driver");
//        } catch (ClassNotFoundException e1) {
//            e1.printStackTrace();
//            System.exit(-1);
//        }
        // lock mode = 1: serializable
        String str_conn = String.format("jdbc:h2:tcp://%s/~/%s;LOCK_MODE=1", url, Config.get().H2_DATABASE_NAME);;
        // jdbc:h2:tcp://34.148.93.160/~/test2", "sa", ""

        Connection conn = DriverManager.getConnection(
                str_conn,
                Config.get().H2_USERNAME,
                Config.get().H2_PASSWORD);

        return conn;
    }

    //===== status managment ======

    public NewSQLConnection getConn4currentThread() {
        long threadId = Thread.currentThread().getId();
        if (!tid2conn.containsKey(threadId)) {
            H2Connection conn = new H2Connection(nextTxnId);
            tid2conn.put(threadId, conn);
            System.out.println("[INFO] tid[" + threadId + "] starts a H2 connection at [" + conn.url + ":" + conn.port + "]");
        }
        return tid2conn.get(threadId);
    }

    public void ReportServerDown() {
        long threadId = Thread.currentThread().getId();
        assert tid2conn.containsKey(threadId);
        tid2conn.remove(threadId);
    }

    // ======== connection ========

    class H2Connection extends NewSQLConnection {

        public H2Connection(AtomicLong tid) {
            super(tid);
        }

        @Override
        public boolean insert(Object txn, String table, String keyStr, String value) throws KvException, TxnException {
            return this.upsert(txn, table, keyStr, value);
        }

        public boolean delete(Object txn, String table, String keyStr) throws KvException, TxnException {
            assert conn != null;
            assert (Long) txn == currTxnId && currTxnId != -1;
            num_op++;

            // tricky part
            Object key = keyStr2Long(keyStr);

            PreparedStatement st;
            try {
                st = conn.prepareStatement("DELETE FROM " + table + " WHERE ID = ?");
                if(key instanceof Long)
                    st.setLong(1, (Long)key);
                else
                    st.setString(1, (String)key);
                int updatedRows = st.executeUpdate();
                if (updatedRows == 0) {
                    return false;
                }
            } catch (SQLException e) {
                String errMsg = e.getMessage();
                if (errMsg.contains("could not serialize access due to concurrent update")) {
                    throw new TxnAbortException(e.getMessage());
                }
                throw new TxnException(e.getMessage());
            }
            return true;
        }

        public String get(Object txn, String table, String keyStr) throws KvException, TxnException {
            assert conn != null;
            assert (Long) txn == currTxnId && currTxnId != -1;
            num_op++;

            // tricky part
            Object key = keyStr2Long(keyStr);

            PreparedStatement st;
            String value = null;
            try {
                st = conn.prepareStatement("SELECT val FROM " + table + " WHERE ID = ?");
                if(key instanceof Long)
                    st.setLong(1, (Long)key);
                else
                    st.setString(1, (String)key);
                ResultSet rs = st.executeQuery();
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                    value = rs.getString("val");
                    //value = new String(rs.getBytes("value"));
                }
                assert rowCount == 0 || rowCount == 1;
            } catch (SQLException e) {
                String errMsg = e.getMessage();
                if (errMsg.contains("could not serialize access due to read/write dependencies")) {
                    throw new TxnAbortException(e.getMessage());
                }
                throw new TxnException(e.getMessage());
            }
            return value;
        }

        public boolean set(Object txn, String table, String keyStr, String value) throws KvException, TxnException {
            return this.upsert(txn, table, keyStr, value);
        }

        private boolean upsert(Object txn, String table, String keyStr, String value) throws KvException, TxnException {
            assert conn != null;
            assert (Long) txn == currTxnId && currTxnId != -1;
            num_op++;

            // TODO: cannot run RUBIS since its keys are String? Why
            // tricky part
            Object key = keyStr2Long(keyStr);

            try {
                Statement stmt = conn.createStatement();
                String sqlstmt = String.format("MERGE INTO " + table + " KEY(ID) VALUES (" +
                        (key instanceof Long?"%d":"'%s'") + ", '%s')", key, value);
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
            }
            return true;
        }

        public Map<Long, String> range(Object txn, String table, String key1Str, String key2Str) throws KvException, TxnException {
            assert conn != null;
            assert (Long) txn == currTxnId && currTxnId != -1;
            num_op++;

            // tricky part
            Object key1 = keyStr2Long(key1Str);

            // tricky part
            Object key2 = keyStr2Long(key2Str);

            PreparedStatement st = null;

            Map<Long, String> results = new HashMap<>();
            Map<String, String> results_str = new HashMap<>();
            try {
                if(key1Str != null && key2Str != null){
                    st = conn.prepareStatement("SELECT ID, val FROM " + table + " WHERE ID >= ? AND ID <= ?");
                    if(key1 instanceof Long){
                        st.setLong(1, (Long)key1);
                        st.setLong(2, (Long)key2);
                    } else{
                        st.setString(1, (String)key1);
                        st.setString(2, (String)key2);
                    }
                } else if(key1Str != null && key2Str == null){
                    st = conn.prepareStatement("SELECT ID, val FROM " + table + " WHERE ID >= ?");
                    if(key1 instanceof Long){
                        st.setLong(1, (Long)key1);
                    } else{
                        st.setString(1, (String)key1);
                    }
                }
                else if(key1Str == null && key2Str != null){
                    st = conn.prepareStatement("SELECT ID, val FROM " + table + " WHERE ID <= ?");
                    if(key2 instanceof Long){
                        st.setLong(2, (Long)key2);
                    } else{
                        st.setString(2, (String)key2);
                    }
                }
                else{
                    st = conn.prepareStatement("SELECT ID, val FROM " + table);
                }

                ResultSet rs = st.executeQuery();
                int rowCount = 0;

                // TODO: modify here to support multiple rows; filter into dead_values and real values? don't filter here
                long key;
                String key_str = null;
                String val = null;
                while (rs.next()) {
                    rowCount++;
                    if(Config.get().KEY_TYPE == 0){
                        key_str = rs.getString("ID");
                        val = rs.getString("val");
                        results_str.put(key_str, val);
                    } else if(Config.get().KEY_TYPE == 1){
                        key = rs.getLong("ID");
                        val = rs.getString("val");
                        results.put(key, val);
                    }
                }
//				assert rowCount == 0 || rowCount == 1;
            } catch (SQLException e) {
                // org.postgresql.util.PSQLException: ERROR: Operation failed. Try again.: Unknown transaction, could be recently aborted: 3c71620e-ecec-49cd-b13c-ba901e979df5
                String errMsg = e.getMessage();
                if (errMsg.contains("could not serialize access due to read/write dependencies")) {
                    throw new TxnAbortException(e.getMessage());
                }
                throw new TxnException(e.getMessage());
            }

            return results;
        }

        public Object begin() throws KvException, TxnException {
            assert conn != null;
            assert currTxnId == -1; // last transaction is finished
            currTxnId = nextTxnId.getAndIncrement();
//			try {
//				writeBuffer = conn.createStatement();
//			} catch (SQLException e) {
//				e.printStackTrace();
//				System.exit(-1);
//			}
            num_op++;
            return currTxnId;
        }

        public boolean commit(Object txn, boolean isInitial, boolean isFinal) throws KvException, TxnException {
            assert conn != null;
            assert (Long) txn == currTxnId && currTxnId != -1;
            num_op++;

            try {
//				int[] res = writeBuffer.executeBatch();
//				assert res.length == writeBufferSize;
//				for(int i : res) {
//					assert i == 1;
//				}
                conn.commit();
//				writeBufferSize = 0;
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
//				writeBuffer.clearBatch();
//				writeBufferSize = 0;
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
//				writeBuffer.clearBatch();
//				writeBufferSize = 0;
                conn.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return true;
        }
    }
}

