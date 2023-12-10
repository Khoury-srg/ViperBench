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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JepsenLogger implements Logger{
    private static final String log_dir = Config.get().COBRA_JEPSEN_LOG;
    private Path jepsen_log_path = null;
    private long tid = -1;
    private static Map<Long, JepsenLogger> instances = Collections.synchronizedMap(new HashMap<Long, JepsenLogger>());

    // Jepsen log buffer
    private ArrayList<JepsenMop> jepsenOpBuffer = new ArrayList<JepsenMop>();

    private JepsenLogger(long tid) {
        this.tid = tid;
        Path dir_path = Paths.get(log_dir);
        if (Files.notExists(dir_path)) {
            try {
                Files.createDirectories(dir_path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        jepsen_log_path = Paths.get(log_dir + "Jep" + this.tid + ".log");
    }

    public static JepsenLogger getInstance() {
        // one instance for one thread
        long tid = Thread.currentThread().getId();
        if (!instances.containsKey(tid)) {
            JepsenLogger one = new JepsenLogger(tid);
            instances.put(tid, one);
        }
        return instances.get(tid);
    }

    /**
     * add a start JepsenMop to the buffer of operations
     *
     * @param  txnid  transaction id
     */
    public void txnStart(long txnid) {
        JepsenMop op = new JepsenMop(OP_TYPE.START_TXN,  null, -1, -1,
                false, false, null);
        jepsenOpBuffer.add(op);
    }

    /**
     * add an abort JepsenMop to the buffer of operations
     *
     * @param  txnid  transaction id
     */
    public void txnAbort(long txnid) {
        //info(toOplogEntry(OP_TYPE.ABORT_TXN, txnid, 0, 0, 0));
        // abandon the opLogBuffer
        jepsenOpBuffer.clear();
    }

    /**
     * add a commit JepsenMop to the buffer of operations
     *
     * @param  txnid  transaction id
     */
    public void txnCommitPre(long txnid, long request_timestamp) {
        // add the commit operation to buffer
        jepsenOpBuffer.add(new JepsenMop(OP_TYPE.COMMIT_TXN, null,
                -1, -1,false, false, null));
    }

    /**
     * dump the operation in the buffer (current transaction) into local files, and empty the buffer of operations
     *
     * @param  txnid  transaction id
     */
    public void txnCommitPost(long txnid, long res_timestamp, boolean isInitial, boolean isFinal) {
//        assert jepsenOpBuffer.size() > 2; // this txn should contain at least i/w/r/range
//        if(jepsenOpBuffer.get(1).key1 == Config.get().EPOCH_KEY){ // gc txn
//            jepsenOpBuffer.clear();
//            return;
//        }
        JepsenMop lastOp = jepsenOpBuffer.get(jepsenOpBuffer.size()-1);
        assert lastOp.op_type == OP_TYPE.COMMIT_TXN;
//        lastOp.res_timestamp = res_timestamp;
        if (Config.get().LOCAL_LOG) {
            // write to the local log
            localTxnCommit();
        }

        // clear opLogBuffer
        jepsenOpBuffer.clear();
    }

    public void txnRead(String key, long value, boolean is_dead,
                        long request_timestamp, long response_timestamp) {
        // NOTE: the sequence of args is not the same
        JepsenMop op = new JepsenMop(OP_TYPE.READ, key, value, -1, is_dead,false, null);
        jepsenOpBuffer.add(op);
    }

    public void txnWrite(String key, long value, boolean update_succ,
                         long request_timestamp, long response_timestamp) {
        JepsenMop op = new JepsenMop(OP_TYPE.WRITE, key, value, -1,false,update_succ, null);
        jepsenOpBuffer.add(op);
    }

//    public void txnInsert(String key, long write_value) {
//        JepsenMop op = new JepsenMop(OP_TYPE.INSERT, key, write_value, -1, false, false, null);
//        jepsenOpBuffer.add(op);
//    }

    public void txnInsert(String key, long write_value, long read_value,
                          boolean is_dead, boolean succ,
                          long request_timestamp, long response_timestamp) {
        JepsenMop op = new JepsenMop(OP_TYPE.INSERT, key, write_value, read_value, is_dead, succ, null);
        jepsenOpBuffer.add(op);
    }

//    public void txnDelete(String key) {
//        JepsenMop op = new JepsenMop(OP_TYPE.DELETE, key, -1, -1, false, false, null);
//        jepsenOpBuffer.add(op);
//    }

    public void txnDelete(String key, long write_value, long read_value,
                          boolean is_dead, boolean succ, long request_timestamp,
                          long response_timestamp) {
        JepsenMop op = new JepsenMop(OP_TYPE.DELETE, key, write_value, read_value, is_dead, succ, null);
        jepsenOpBuffer.add(op);
    }

    public void txnRangeQuery(String key1, String key2,
                              Map<Long, Long> real_vals,
                              Map<Long, Long> dead_vals,
                              long request_timestamp,
                              long response_timestamp) {
        JepsenMop op = new JepsenMop(OP_TYPE.RANGE, key1, -1,-1, false,false, key2);
        // filter values into real_vals and dead_vals
        op.setValuesArray(real_vals, dead_vals);
        jepsenOpBuffer.add(op);
    }

//    public void txnFinalState(Map<String, Long> val_hashes) {
//        JepsenMop op = new JepsenMop(OP_TYPE.FINAL, null, -1,-1, false,false, null);
//        // filter values into real_vals and dead_vals
//        op.setValuesArray(val_hashes, null);
//        jepsenOpBuffer.add(op);
//    }

    // dump the log into local files
    private void localTxnCommit() {
//        assert jepsenOpBuffer.size() > 2;
        StringBuilder ret = new StringBuilder();
        long timeStamp = MyTimestamp.getTimestamp();
        ret.append("{");

        ret.append(":value: [");
        for (int i = 0; i < jepsenOpBuffer.size(); i++) {
            JepsenMop curr_op = jepsenOpBuffer.get(i);
            if(i == 0){
                assert curr_op.op_type == OP_TYPE.START_TXN;
                continue;
            }

            if(i == jepsenOpBuffer.size() - 1){
                assert curr_op.op_type == OP_TYPE.COMMIT_TXN;
                continue;
            }

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
            Files.write(jepsen_log_path, msg.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void txnSelect2(String sql, Object[] rs) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'txnSelect2'");
    }

    @Override
    public void txnUpdate2(String sql, int update_cnt) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'txnUpdate2'");
    }

    @Override
    public void txnInsert2(String sql) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'txnInsert2'");
    }

    @Override
    public void txnDelete2(String sql) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'txnDelete2'");
    }


}
