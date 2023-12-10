package kv_interfaces.instrument;

import java.sql.ResultSet;
import java.util.Map;

public interface Logger {
    public void txnStart(long txnid) ;
    public void txnAbort(long txnid);
    public void txnCommitPre(long txnid, long req_timestamp) ;
    public void txnCommitPost(long txnid, long res_timestamp, boolean isInitial, boolean isFinal) ;

    public void txnRead(String key, long value, boolean is_dead,
                        long req_timestamp, long res_timestamp) ;

    public void txnWrite(String key, long value, boolean update_succ,
                         long req_timestamp, long res_timestamp);

    public void txnInsert(String key, long write_value, long read_value,
                          boolean is_dead, boolean succ,
                          long req_timestamp, long res_timestamp) ;

    public void txnDelete(String key, long write_value, long read_value,
                          boolean is_dead, boolean succ,
                          long req_timestamp, long res_timestamp);

    public void txnRangeQuery(String key1, String key2,
                              Map<Long, Long> real_vals,
                              Map<Long, Long> dead_vals,
                              long req_timestamp, long res_timestamp);

    // SQL logging api
    public void txnSelect2(String sql, Object[] rs) ;

    public void txnUpdate2(String sql, int update_cnt);

    public void txnInsert2(String sql);

    public void txnDelete2(String sql);
}
