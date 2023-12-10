package kv_interfaces.instrument;

import kv_interfaces.OP_TYPE;
import main.Config;
import main.MyTimestamp;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.util.*;

public class SQLLogger implements Logger{
    private static final String log_dir = Config.get().COBRA_SQL_LOG;
    private Path log_path = null;
    private long tid = -1;
    private static Map<Long, SQLLogger> instances = 
        Collections.synchronizedMap(new HashMap<Long, SQLLogger>());

    // Jepsen log buffer
    private ArrayList<SQLMop> jsonOpBuffer = new ArrayList<>();

    private SQLLogger(long tid) {
        this.tid = tid;
        Path dir_path = Paths.get(log_dir);
        if (Files.notExists(dir_path)) {
            try {
                Files.createDirectories(dir_path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        log_path = Paths.get(log_dir + "sql" + this.tid + ".log");
    }

    public static SQLLogger getInstance() {
        // one instance for one thread
        long tid = Thread.currentThread().getId();
        if (!instances.containsKey(tid)) {
            SQLLogger one = new SQLLogger(tid);
            instances.put(tid, one);
        }
        return instances.get(tid);
    }

    /**
     *
     * @param update_cnt
     * @param v2s: an array of column v2
     * @return
     */
    private SQLMop createMop(OP_TYPE op_type, String sql,
        int update_cnt, Object[] v2s){
        SQLMop mop = new SQLMop(op_type, sql, v2s);
        return mop;
    }

    /**
     * add a start mop to the buffer of operations
     *
     * @param  txnid  transaction id
     */
    public void txnStart(long txnid) {
        SQLMop op = createMop(OP_TYPE.START_TXN, null, -1, null);
        jsonOpBuffer.add(op);
    }

    /**
     * add an abort mop to the buffer of operations
     *
     * @param  txnid  transaction id
     */
    public void txnAbort(long txnid) {
        jsonOpBuffer.clear();
    }

    /**
     * add a commit mop to the buffer of operations
     *
     * @param  txnid  transaction id
     */
    public void txnCommitPre(long txnid, long req_timestamp) {
        // add the commit operation to buffer
        SQLMop op = createMop(OP_TYPE.COMMIT_TXN, null, -1, null);
        jsonOpBuffer.add(op);
    }

    /**
     * dump the operation in the buffer (current transaction) into local files, and empty the buffer of operations
     *
     * @param  txnid  transaction id
     */
    public void txnCommitPost(long txnid, long res_timestamp, boolean isInitial, boolean isFinal) {
        SQLMop lastOp = jsonOpBuffer.get(jsonOpBuffer.size()-1);
        assert lastOp.opType.equals("commit");

        if (Config.get().LOCAL_LOG) {
            // write to the local log
            localTxnCommit();
        }

        // clear opLogBuffer
        jsonOpBuffer.clear();
    }

    public void txnRead(String key, long value, boolean is_dead,
                        long req_timestamp, long res_timestamp) {
        throw new UnsupportedOperationException("Unimplemented method 'txnInsert2'");
    }

    public void txnWrite(String key, long value, boolean update_succ,
                         long req_timestamp, long res_timestamp) {
        throw new UnsupportedOperationException("Unimplemented method 'txnInsert2'");
    }

    public void txnInsert(String key, long write_value, long read_value,
                          boolean is_dead, boolean succ,
                          long req_timestamp, long res_timestamp) {
        throw new UnsupportedOperationException("Unimplemented method 'txnInsert2'");
    }

//    public void txnDelete(String key) {
//        Mop op = new Mop(OP_TYPE.DELETE, key, -1, -1, false, false, null);
//        jepsenOpBuffer.add(op);
//    }

    public void txnDelete(String key, long write_value, long read_value,
                          boolean is_dead, boolean succ,
                          long req_timestamp, long res_timestamp) {
        throw new UnsupportedOperationException("Unimplemented method 'txnInsert2'");
    }

    public void txnRangeQuery(String key1, String key2,
                              Map<Long, Long> real_vals,
                              Map<Long, Long> dead_vals,
                              long req_timestamp,
                              long res_timestamp) {
        throw new UnsupportedOperationException("Unimplemented method 'txnInsert2'");
    }

    // dump the log into local files
    private void localTxnCommit() {
        StringBuilder ret = new StringBuilder();
        ret.append(String.format("{\"size\": %d, ", jsonOpBuffer.size()-1));
        ret.append("\"value\": [");
        for (int i = 1; i < jsonOpBuffer.size(); i++) {
            SQLMop curr_op = jsonOpBuffer.get(i);

            if(i == 1){
                ret.append(curr_op.toString());
            } else {
                ret.append(", " + curr_op.toString() );
            }
        }
        ret.append("]}\n");
        String res = ret.toString();

        write2clientLog(res);
    }

    private void write2clientLog(String msg) {
        try {
            Files.write(log_path, msg.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void txnSelect2(String sql, Object[] rs) {
        SQLMop mop = createMop(OP_TYPE.READ, sql, -1, rs);
        jsonOpBuffer.add(mop);
    }

    @Override
    public void txnUpdate2(String sql, int update_cnt) {
        SQLMop mop = createMop(OP_TYPE.WRITE, sql, -1, null);
        jsonOpBuffer.add(mop);
    }

    @Override
    public void txnInsert2(String sql) {
        SQLMop mop = createMop(OP_TYPE.INSERT, sql, -1, null);
        jsonOpBuffer.add(mop);
    }

    @Override
    public void txnDelete2(String sql) {
        SQLMop mop = createMop(OP_TYPE.DELETE, sql, -1, null);
        jsonOpBuffer.add(mop);
    }
}