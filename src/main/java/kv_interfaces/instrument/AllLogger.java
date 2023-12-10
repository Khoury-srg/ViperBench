package kv_interfaces.instrument;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AllLogger implements Logger {
    private JepsenLogger jepsenLogger = JepsenLogger.getInstance();
    private JsonLogger jsonLogger = JsonLogger.getInstance();
    private SQLLogger sqlLogger = SQLLogger.getInstance();

    private long tid = -1;
    private static Map<Long, AllLogger> instances =
            Collections.synchronizedMap(new HashMap<Long, AllLogger>());

    public static AllLogger getInstance() {
        // one instance for one thread
        long tid = Thread.currentThread().getId();
        if (!instances.containsKey(tid)) {
            AllLogger one = new AllLogger(tid);
            instances.put(tid, one);
        }
        return instances.get(tid);
    }

    private AllLogger(long tid) {
        this.tid = tid;
    }

    @Override
    public void txnStart(long txnid) {
        jepsenLogger.txnStart(txnid);
        jsonLogger.txnStart(txnid);
        sqlLogger.txnStart(txnid);
    }

    @Override
    public void txnAbort(long txnid) {
        jepsenLogger.txnAbort(txnid);
        jsonLogger.txnAbort(txnid);
        sqlLogger.txnAbort(txnid);
    }

    @Override
    public void txnCommitPre(long txnid, long request_timestamp) {
        jepsenLogger.txnCommitPre(txnid, request_timestamp);
        jsonLogger.txnCommitPre(txnid, request_timestamp);
        sqlLogger.txnCommitPre(txnid, request_timestamp);
    }

    @Override
    public void txnCommitPost(long txnid, long res_timestamp,
                              boolean isInitial, boolean isFinal) {
        jepsenLogger.txnCommitPost(txnid, res_timestamp, isInitial, isFinal);
        jsonLogger.txnCommitPost(txnid, res_timestamp, isInitial, isFinal);
        sqlLogger.txnCommitPost(txnid, res_timestamp, isInitial, isFinal);
    }

    @Override
    public void txnRead(String key, long value, boolean is_dead,
                        long request_timestamp, long response_timestamp) {
        jepsenLogger.txnRead(key, value, is_dead, request_timestamp, response_timestamp);
        jsonLogger.txnRead(key, value, is_dead, request_timestamp, response_timestamp);
    }

    @Override
    public void txnWrite(String key, long value, boolean update_succ,
                         long request_timestamp, long response_timestamp) {
        jepsenLogger.txnWrite(key, value, update_succ, request_timestamp, response_timestamp);
        jsonLogger.txnWrite(key, value, update_succ, request_timestamp, response_timestamp);
    }

    @Override
    public void txnInsert(String key, long write_value, long read_value,
                          boolean is_dead, boolean succ,
                          long request_timestamp, long response_timestamp) {
        jepsenLogger.txnInsert(key, write_value, read_value, is_dead, succ, request_timestamp, response_timestamp);
        jsonLogger.txnInsert(key, write_value, read_value, is_dead, succ, request_timestamp, response_timestamp);
    }

    @Override
    public void txnDelete(String key, long write_value, long read_value,
                          boolean is_dead, boolean succ,
                          long request_timestamp, long response_timestamp) {
        jepsenLogger.txnDelete(key, write_value, read_value, is_dead, succ, request_timestamp, response_timestamp);
        jsonLogger.txnDelete(key, write_value, read_value, is_dead, succ, request_timestamp, response_timestamp);
    }

    @Override
    public void txnRangeQuery(String key1, String key2,
                              Map<Long, Long> real_vals,
                              Map<Long, Long> dead_vals,
                              long request_timestamp, long response_timestamp) {
        jepsenLogger.txnRangeQuery(key1, key2, real_vals, dead_vals, request_timestamp, response_timestamp);
        jsonLogger.txnRangeQuery(key1, key2, real_vals, dead_vals, request_timestamp, response_timestamp);
    }

    @Override
    public void txnSelect2(String sql, Object[] rs) {
        sqlLogger.txnSelect2(sql, rs);
    }

    @Override
    public void txnUpdate2(String sql, int update_cnt) {
        sqlLogger.txnUpdate2(sql, update_cnt);
    }

    @Override
    public void txnInsert2(String sql) {
        sqlLogger.txnInsert2(sql);
    }

    @Override
    public void txnDelete2(String sql) {
        sqlLogger.txnDelete2(sql);
    }
}
