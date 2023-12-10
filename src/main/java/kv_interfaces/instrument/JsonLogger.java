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

public class JsonLogger implements Logger{
    private static final String log_dir = Config.get().COBRA_JSON_LOG;
    private Path log_path = null;
    private long tid = -1;
    private static Map<Long, JsonLogger> instances = Collections.synchronizedMap(new HashMap<Long, JsonLogger>());

    // Jepsen log buffer
    private ArrayList<JSONMop> jsonOpBuffer = new ArrayList<JSONMop>();

    private JsonLogger(long tid) {
        this.tid = tid;
        Path dir_path = Paths.get(log_dir);
        if (Files.notExists(dir_path)) {
            try {
                Files.createDirectories(dir_path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        log_path = Paths.get(log_dir + "J" + this.tid + ".log");
    }

    public static JsonLogger getInstance() {
        // one instance for one thread
        long tid = Thread.currentThread().getId();
        if (!instances.containsKey(tid)) {
            JsonLogger one = new JsonLogger(tid);
            instances.put(tid, one);
        }
        return instances.get(tid);
    }

    private JSONMop createMop(int flag, OP_TYPE op_type, String key,
                              long value, long readV, boolean isDead,
                              boolean update_cuss, String key2,
                              long req_timestamp, long res_timestamp){
        JSONMop op = null;
        if(flag == 1){
            op = new JSONMop1(op_type,  key, value, readV,
                    isDead, update_cuss, key2);
        } else if (flag == 2){
            op = new JSONMop2(op_type,  key, value, readV,
                    isDead, update_cuss, key2, req_timestamp, res_timestamp);
        }
        return op;
    }

    /**
     * add a start mop to the buffer of operations
     *
     * @param  txnid  transaction id
     */
    public void txnStart(long txnid) {
        JSONMop op = createMop(Config.get().JSON, OP_TYPE.START_TXN,  null, -1, -1,
                false, false, null, -1, -1);

        jsonOpBuffer.add(op);
    }

    /**
     * add an abort mop to the buffer of operations
     *
     * @param  txnid  transaction id
     */
    public void txnAbort(long txnid) {
        //info(toOplogEntry(OP_TYPE.ABORT_TXN, txnid, 0, 0, 0));
        // abandon the opLogBuffer
        jsonOpBuffer.clear();
    }

    /**
     * add a commit mop to the buffer of operations
     *
     * @param  txnid  transaction id
     */
    public void txnCommitPre(long txnid, long req_timestamp) {
        // add the commit operation to buffer
        JSONMop op = createMop(Config.get().JSON, OP_TYPE.COMMIT_TXN, null,
                -1, -1,false, false, null,
                req_timestamp, -1);
        jsonOpBuffer.add(op);
    }

    /**
     * dump the operation in the buffer (current transaction) into local files, and empty the buffer of operations
     *
     * @param  txnid  transaction id
     */
    public void txnCommitPost(long txnid, long res_timestamp,
                              boolean isInitial, boolean isFinal) {
        JSONMop lastOp = jsonOpBuffer.get(jsonOpBuffer.size()-1);

        assert lastOp.op_type == OP_TYPE.COMMIT_TXN;
        lastOp.res_timestamp = res_timestamp;
        if (Config.get().LOCAL_LOG) {
            // write to the local log
            localTxnCommit(isInitial, isFinal);
        }

        // clear opLogBuffer
        jsonOpBuffer.clear();
    }

    public void txnRead(String key, long value, boolean is_dead,
                        long req_timestamp, long res_timestamp) {
        // NOTE: the sequence of args is not the same
        JSONMop op = createMop(Config.get().JSON, OP_TYPE.READ,
                key, value, -1, is_dead,false, null,
                req_timestamp, res_timestamp);
        jsonOpBuffer.add(op);
    }

    public void txnWrite(String key, long value, boolean update_succ,
                         long req_timestamp, long res_timestamp) {
        JSONMop op = createMop(Config.get().JSON,
                OP_TYPE.WRITE, key, value, -1,
                false,update_succ, null,
                req_timestamp, res_timestamp);
        jsonOpBuffer.add(op);
    }

//    public void txnInsert(String key, long write_value) {
//        Mop op = new Mop(OP_TYPE.INSERT, key, write_value, -1, false, false, null);
//        jepsenOpBuffer.add(op);
//    }

    public void txnInsert(String key, long write_value, long read_value,
                          boolean is_dead, boolean succ,
                          long req_timestamp, long res_timestamp) {
        JSONMop op = createMop(Config.get().JSON, OP_TYPE.INSERT,
                key, write_value, read_value, is_dead, succ, null,
                req_timestamp, res_timestamp);

        jsonOpBuffer.add(op);
    }

//    public void txnDelete(String key) {
//        Mop op = new Mop(OP_TYPE.DELETE, key, -1, -1, false, false, null);
//        jepsenOpBuffer.add(op);
//    }

    public void txnDelete(String key, long write_value, long read_value,
                          boolean is_dead, boolean succ,
                          long req_timestamp, long res_timestamp) {

        JSONMop op = createMop(Config.get().JSON, OP_TYPE.DELETE,
                key, write_value, read_value, is_dead, succ, null,
                req_timestamp, res_timestamp);
        jsonOpBuffer.add(op);
    }

    public void txnRangeQuery(String key1, String key2,
                              Map<Long, Long> real_vals,
                              Map<Long, Long> dead_vals,
                              long req_timestamp,
                              long res_timestamp) {
        JSONMop op = createMop(Config.get().JSON, OP_TYPE.RANGE,
                key1, -1,-1, false,false, key2,
                req_timestamp, res_timestamp);
        // filter values into real_vals and dead_vals
        op.setValuesArray(real_vals, dead_vals);
        jsonOpBuffer.add(op);
    }

    // dump the log into local files
    private void localTxnCommit(boolean isInitial, boolean isFinal) {
//        assert jepsenOpBuffer.size() > 2;
        StringBuilder ret = new StringBuilder();
        long preBeginTimestamp = jsonOpBuffer.get(1).req_timestamp;
        long postBeginTimestamp = jsonOpBuffer.get(1).res_timestamp;
        long preCommitTimestamp = jsonOpBuffer.get(jsonOpBuffer.size()-1).req_timestamp;
        long postCommitTimestamp = jsonOpBuffer.get(jsonOpBuffer.size()-1).res_timestamp;

        ret.append("{");
        if(Config.get().PRINT_TS){
            ret.append(String.format("\"preBeginTS\": %d, " +
                            "\"postBeginTS\": %d, " +
                            "\"preCommitTS\": %d, " +
                            "\"postCommitTS\": %d, ",
                    preBeginTimestamp,
                    postBeginTimestamp,
                    preCommitTimestamp,
                    postCommitTimestamp));
        }
//        ret.append("{\"opType\": " + jepsenOpBuffer.get(0).op_type +", ");
        ret.append("\"value\": [");
        for (int i = 1; i < jsonOpBuffer.size() - 1; i++) {
            JSONMop curr_op = jsonOpBuffer.get(i);
            String mopStr = curr_op.toString();

            if(i == 1){
                assert mopStr != null && !mopStr.isEmpty();
                ret.append(mopStr);
            } else {
                ret.append(", " + mopStr);
            }
        }

        ret.append("]");
        if(isInitial || isFinal){
            ret.append(String.format(
                """
                , "isInitial": %b, "isFinal": %b""", isInitial, isFinal));
        }
        ret.append("}\n");
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