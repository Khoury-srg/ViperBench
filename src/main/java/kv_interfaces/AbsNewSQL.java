package kv_interfaces;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import bench.Crack.WriteSkewBench;
import bench.Tables;
import bench.simpleSQL.SimpleSQLConstants;
import kvstore.exceptions.DupInsertException;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnAbortException;
import kvstore.exceptions.TxnException;
import main.Config;

public abstract class AbsNewSQL implements KvInterface {
    // init status
    // unique txn id
    public AtomicLong nextTxnId;
    // connection per thread
    protected Map<Long, NewSQLConnection> tid2conn;
    // ports
    protected final String[] possible_urls;
    protected final int[] possible_ports;
    protected String[] tables = {WriteSkewBench.TABLE_A, WriteSkewBench.TABLE_B};

    // customized parts
    public abstract Connection getConnection(String url, int port) throws SQLException;

    public abstract NewSQLConnection getConn4currentThread();

    public abstract void ReportServerDown();

    // ===== singleton =====

    AbsNewSQL(String[] urls, int[] ports) {
        nextTxnId = new AtomicLong(((long) Config.get().CLIENT_ID) << 32);
        tid2conn = new ConcurrentHashMap<Long, NewSQLConnection>();
        this.possible_urls = urls;
        this.possible_ports = ports;
    }

    public String Status() {
        String status = "STATUS: ";
        try {
            StringBuilder sb = new StringBuilder(status);
            for (long tid : tid2conn.keySet()) {
                sb.append("T[" + tid + "][" + tid2conn.get(tid).url + ":" + tid2conn.get(tid).port + "] ");
            }
            status = sb.toString();
        } catch (Exception e) {
            System.out.print("[ERROR] print status meet some error: " + e.getMessage());
        }
        return status;
    }

    // ======== APIs ===========
    public Object begin() throws KvException, TxnException {
        return getConn4currentThread().begin();
    }

    @Override
    public boolean commit(Object txn, boolean isInitial, boolean isFinal) throws KvException, TxnException {
        return getConn4currentThread().commit(txn);
    }

    @Override
    public boolean abort(Object txn) throws KvException, TxnException {
        return getConn4currentThread().abort(txn);
    }

    @Override
    public boolean insert(Object txn, String key, String value) throws KvException, TxnException {
        // String[] tokens = DecodeTableKey(key);
        // assert tokens.length == 2;
        // temporarily only use table A
        // return getConn4currentThread().insert(txn, tables[0], tokens[1], value);
        Map<String, String> ret = Tables.DecodeTableKey(key, tables);
        return getConn4currentThread().insert(txn, ret.get("table"), key, value);
    }

    @Override
    public boolean delete(Object txn, String key) throws KvException, TxnException {
        // String[] tokens = DecodeTableKey(key);
        // assert tokens.length == 2;
        // return getConn4currentThread().delete(txn, tables[0], tokens[1]);
        // TODO you generate a random value
        Map<String, String> ret = Tables.DecodeTableKey(key, tables);
        return getConn4currentThread().delete(txn, ret.get("table"), key);
    }

    @Override
    public String get(Object txn, String key) throws KvException, TxnException {
        // String[] tokens = DecodeTableKey(key);
        // assert tokens.length == 2;
        // return getConn4currentThread().get(txn, tables[0], tokens[1]);
        Map<String, String> ret = Tables.DecodeTableKey(key, tables); // only for Crack bench
        return getConn4currentThread().get(txn, ret.get("table"), key);
    }

    @Override
    public boolean set(Object txn, String key, String value) throws KvException, TxnException {
        // String[] tokens = DecodeTableKey(key);
        // assert tokens.length == 2;
        // return getConn4currentThread().set(txn, tables[0], key, value);
        Map<String, String> ret = Tables.DecodeTableKey(key, tables); // only for Crack bench
        return getConn4currentThread().set(txn, ret.get("table"), key, value);
    }

    @Override
    public Map<Long, String> range(Object txn, String key1, String key2) throws KvException, TxnException {
        // TODO:

        // return getConn4currentThread().range(txn, tables[0], key1, key2);
        Map<String, String> ret1 = Tables.DecodeTableKey(key1, tables);
        Map<String, String> ret2 = Tables.DecodeTableKey(key2, tables);
        assert ret1.get("table") == ret2.get("table");
        return getConn4currentThread().range(txn, ret1.get("table"), key1, key2);
    }

    @Override
    public boolean rollback(Object txn) {
        return getConn4currentThread().rollback(txn);
    }

    @Override
    public boolean isalive(Object txn) {
        return getConn4currentThread().isalive(txn);
    }


    @Override
    public Object[] executeSQL(Object txn, String sql, SimpleSQLConstants.TASK_TYPE taskType,
                               Class cls) throws KvException,
            TxnException {
        return getConn4currentThread().executeSQL(txn, sql, taskType, cls);
    }

    @Override
    public long getTxnId(Object txn) {
        /**
         * We use per thread singleton for SQL connection. Each thread must only have no
         * more than one ongoing transaction. So it is useless to specify txn in the
         * parameter of all those operations(begin, set, put,...) so the txn is just
         * transaction id and we can always check whether the passed-in txnid is the
         * same with the ongoing one in our state.
         */
        return (Long) txn;
    }

    @Override
    public Object getTxn(long txnid) {
        return txnid;
    }

    @Override
    public boolean isInstrumented() {
        return false;
    }

    /**
     * @return An integer, which is the sum of operation counts of all connections
     */
    public int getNumOp() {
        int total_num_op = 0;
        for (NewSQLConnection conn : tid2conn.values()) {
            total_num_op += conn.num_op;
        }
        return total_num_op;
    }

    public void clearNumOp() {
        for (NewSQLConnection conn : tid2conn.values()) {
            conn.num_op = 0;
        }
    }

    // =============================
    abstract class NewSQLConnection {
        // ==== connection states of each thread ========
        protected Connection conn = null;
        protected Statement writeBuffer = null;
        protected int writeBufferSize = 0;
        protected long currTxnId = -1;
        protected int num_op = 0;
        protected AtomicLong nextTxnId;
        protected String url;
        protected int port;

        public NewSQLConnection(AtomicLong tid) {
            nextTxnId = tid;

            while (initConnection() != true) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        private boolean initConnection() {
            int i = (new Random().nextInt(possible_ports.length)) % Config.get().NUM_REPLICA;
            url = possible_urls[i];
            port = possible_ports[i];
            try {
                this.conn = getConnection(url, port);
                this.conn.setAutoCommit(false);
                // be careful that some databases may not support the standard 4 isolation levels.
                this.conn.setTransactionIsolation(Config.get().getIsolationLevel());
                this.writeBuffer = null;
                this.writeBufferSize = 0;
            } catch (Exception e) {
                System.err.println("[ERROR] connection error: " + e.getMessage());
                return false;
            }
            return true;
        }

        protected boolean isInteger(String str) {
            Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
            return pattern.matcher(str).matches();
        }

        protected Object keyStr2Long(String key) {
            if (key == null)
                return null;

            if (isInteger(key))
                return Long.valueOf(key);

            if (!key.startsWith("key"))
                return key;

            int firstDigitIndex = -1;
            for (int i = 0; i < key.length(); i++) {
                if (Character.isDigit(key.charAt(i))) {
                    firstDigitIndex = i;
                    break;
                }
            }
            assert firstDigitIndex != -1;
            return Long.valueOf(key.substring(firstDigitIndex));
        }

        // =============== kv interface =================

        /**
         * In JDBC we don't have to specify the start of a transaction So we just
         * construct a transaction ID. And this is a single thread case, so it's really
         * simple.
         */
        public Object begin() throws KvException, TxnException {
            assert conn != null;
            assert currTxnId == -1; // last transaction is finished
            currTxnId = nextTxnId.getAndIncrement();
            try {
                writeBuffer = conn.createStatement();
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            num_op++;
            return currTxnId;
        }

        public boolean commit(Object txn) throws KvException, TxnException {
            assert conn != null;
            assert (Long) txn == currTxnId && currTxnId != -1;
            num_op++;

            try {
                int[] res = writeBuffer.executeBatch();
                assert res.length == writeBufferSize;
                for (int i : res) {
                    assert i == 1;
                }
                conn.commit();
                writeBufferSize = 0;
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
                writeBuffer.clearBatch();
                writeBufferSize = 0;
                conn.rollback();
                currTxnId = -1;
            } catch (SQLException e) {
                throw new TxnException(e.getMessage());
            }
            return true;
        }

        public boolean insert(Object txn, String table, String key, String value) throws KvException, TxnException {
            assert conn != null;
            assert writeBuffer != null;
            assert (Long) txn == currTxnId && currTxnId != -1;
            num_op++;

            try {
                String sqlstmt = String.format("INSERT INTO " + table + " (key, value) VALUES ('%s', '%s')", key,
                        value);
                writeBuffer.addBatch(sqlstmt);
                writeBufferSize++;
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

        public boolean delete(Object txn, String table, String key) throws KvException, TxnException {
            assert conn != null;
            assert (Long) txn == currTxnId && currTxnId != -1;
            num_op++;

            PreparedStatement st;
            try {
                st = conn.prepareStatement("DELETE FROM " + table + " WHERE key = ?");
                st.setString(1, key);
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

        public String get(Object txn, String table, String key) throws KvException, TxnException {
            assert conn != null;
            assert (Long) txn == currTxnId && currTxnId != -1;
            num_op++;

            PreparedStatement st;
            String value = null;
            try {
                st = conn.prepareStatement("SELECT value FROM " + table + " WHERE key = ?");
                st.setString(1, key);
                ResultSet rs = st.executeQuery();
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                    value = rs.getString("value");
                    // value = new String(rs.getBytes("value"));
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

        public boolean set(Object txn, String table, String key, String value) throws KvException, TxnException {
            assert conn != null;
            assert (Long) txn == currTxnId && currTxnId != -1;
            num_op++;

            try {
                String sqlstmt = String.format("UPDATE " + table + " SET value = '%s' WHERE key = '%s'", value, key);
                writeBuffer.addBatch(sqlstmt);
                writeBufferSize++;
            } catch (SQLException e) {
                String errMsg = e.getMessage();
                if (errMsg.contains("could not serialize access due to")) {
                    throw new TxnAbortException(e.getMessage());
                } else if (errMsg.contains("deadlock detected")) {
                    throw new TxnAbortException(e.getMessage());
                } else {
                    // e.printStackTrace();
                    throw new TxnException(e.getMessage());
                }
            }
            return true;
        }

        public Map<Long, String> range(Object txn, String table, String key1Str, String key2Str)
                throws KvException, TxnException {
            assert conn != null;
            assert (Long) txn == currTxnId && currTxnId != -1;
            num_op++;

            // tricky part
            Object key1 = keyStr2Long(key1Str);
            // if(key1 instanceof Long)
            // key1 = (Long)key1;
            // else
            // key1 = (String) key1;

            // tricky part
            Object key2 = keyStr2Long(key2Str);
            // if(key2 instanceof Long)
            // key2 = (Long)key2;
            // else
            // key2 = (String) key2;

            PreparedStatement st = null;
            // ArrayList<Long> keys = new ArrayList<>();
            // ArrayList<String> values = new ArrayList<>();
            Map<Long, String> results = new HashMap<>();
            Map<String, String> results_str = new HashMap<>();
            try {
                if (key1Str != null && key2Str != null) {
                    st = conn.prepareStatement("SELECT ID, value FROM " + table + " WHERE ID >= ? AND ID <= ?");
                    if (key1 instanceof Long) {
                        st.setLong(1, (Long) key1);
                        st.setLong(2, (Long) key2);
                    } else {
                        st.setString(1, (String) key1);
                        st.setString(2, (String) key2);
                    }
                } else if (key1Str != null && key2Str == null) {
                    st = conn.prepareStatement("SELECT ID, value FROM " + table + " WHERE ID >= ?");
                    if (key1 instanceof Long) {
                        st.setLong(1, (Long) key1);
                    } else {
                        st.setString(1, (String) key1);
                    }
                } else if (key1Str == null && key2Str != null) {
                    st = conn.prepareStatement("SELECT ID, value FROM " + table + " WHERE ID <= ?");
                    if (key2 instanceof Long) {
                        st.setLong(2, (Long) key2);
                    } else {
                        st.setString(2, (String) key2);
                    }
                } else {
                    st = conn.prepareStatement("SELECT ID, value FROM " + table);
                }

                ResultSet rs = st.executeQuery();
                int rowCount = 0;

                // TODO: modify here to support multiple rows; filter into dead_values and real
                // values? don't filter here
                long key;
                String key_str = null;
                String val = null;
                while (rs.next()) {
                    rowCount++;
                    if (Config.get().KEY_TYPE == 0) {
                        key_str = rs.getString("ID");
                        val = rs.getString("value");
                        results_str.put(key_str, val);
                    } else if (Config.get().KEY_TYPE == 1) {
                        key = rs.getLong("ID");
                        val = rs.getString("value");
                        results.put(key, val);
                    }
                    // value = new String(rs.getBytes("value"));
                }
                // assert rowCount == 0 || rowCount == 1;
            } catch (SQLException e) {
                // org.postgresql.util.PSQLException: ERROR: Operation failed. Try again.:
                // Unknown transaction, could be recently aborted:
                // 3c71620e-ecec-49cd-b13c-ba901e979df5
                String errMsg = e.getMessage();
                if (errMsg.contains("could not serialize access due to read/write dependencies")) {
                    throw new TxnAbortException(e.getMessage());
                }
                throw new TxnException(e.getMessage());
            }

            return results;
        }

        // public abstract Map<Long, String> range(Object txn, String table, String
        // key1, String key2) throws KvException, TxnException;

        public boolean rollback(Object txn) {
            assert currTxnId == (Long) txn && currTxnId != -1;
            num_op++;
            currTxnId = -1;
            try {
                writeBuffer.clearBatch();
                writeBufferSize = 0;
                conn.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return true;
        }

        public boolean isalive(Object txn) {
            return (Long) txn == currTxnId;
        }

        // SQL API
        public Object[] executeSQL(Object txn, String sql, SimpleSQLConstants.TASK_TYPE taskType,
                                   Class cls)
                throws TxnException {
            assert conn != null;
            assert (Long) txn == currTxnId && currTxnId != -1;
            num_op++;

            ResultSet rs = null;
            int update_cnt = 0;
            List vals = null;
			Statement stmt = null;
//            sql = String.format(sql, table);

            try {
                stmt = conn.createStatement();
                if (taskType == SimpleSQLConstants.TASK_TYPE.SELECT) {
                    rs = stmt.executeQuery(sql);
                    ResultSetMetaData rsmd = rs.getMetaData();
					String colName = rsmd.getColumnName(1);
//                    String colSimpleName = rsmd.getColumnName(1);
					String colType = rsmd.getColumnTypeName(1);
//					Class<?> colCls = Class.forName(colType);

					vals = new ArrayList();
                    while (rs.next()) {
                        vals.add(rs.getObject(colName));
//                        vals.add(colCls.cast(rs.getObject(colName)));
                    }
					return vals.toArray();
                } else {
                    update_cnt = stmt.executeUpdate(sql);
                }
                return null;
            } catch (SQLException e) {
                String errMsg = e.getMessage();
                if (errMsg.contains("could not serialize access due to read/write dependencies")) {
                    throw new TxnAbortException(e.getMessage());
                }
                throw new TxnException(e.getMessage());
            } finally {
                try {
                    if (rs != null)
                        rs.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

				try{
					if(stmt != null)
						stmt.close();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}

            }
        }
    }
}
