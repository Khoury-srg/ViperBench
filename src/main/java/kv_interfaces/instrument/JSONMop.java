package kv_interfaces.instrument;

import kv_interfaces.OP_TYPE;
import main.Config;

import java.util.HashMap;
import java.util.Map;

public abstract class JSONMop {
    public OP_TYPE op_type;
    protected long req_timestamp;
    protected long res_timestamp;
    //    private long txnid;
    protected String key1;
    protected long value;
    protected long read_v;
    protected boolean is_dead;
    protected boolean update_succ;

    // for range query results
    protected String key2;
    protected Map<Long, Long> real_vals=new HashMap<>(); // ["k1:v1", "k2:v2"]
    protected Map<Long, Long> dead_vals=new HashMap<>();

    // for string keys
    protected Map<String, Long> vals=new HashMap<>();
//    protected boolean isInitial;
//    protected boolean isFinal;

    // {:type :ok, :f :txn, :value [[]]}
    protected boolean isIntegerKey = Config.get().KEY_TYPE == 1;

    public void setValuesArray(Map<Long, Long> real_vals, Map<Long, Long> dead_vals){
        this.real_vals = real_vals;
        this.dead_vals = dead_vals;
    }

    public void setFinalValues(Map<String, Long> vals){
        this.vals = vals;
    }

    protected String values2String(Map<Long, Long> vals){
        StringBuilder s = new StringBuilder();
        s.append("[");

        boolean isFirstKVPair = true;
        for(Map.Entry<Long, Long> entry: vals.entrySet()){
            String tmp = String.format("{\"id\": %d, \"sk\": %d, \"val\": %d}",
                    entry.getKey(), entry.getKey(), entry.getValue());
            if(isFirstKVPair)
                isFirstKVPair = false;
            else
                tmp = ", " + tmp;
            s.append(tmp);
        }
        s.append("]");

        return s.toString();
    }
}
