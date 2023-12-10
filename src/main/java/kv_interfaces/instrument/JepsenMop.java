package kv_interfaces.instrument;

import kv_interfaces.OP_TYPE;
import java.util.HashMap;
import java.util.Map;


public class JepsenMop {
    public OP_TYPE op_type;
    //    private long txnid;
    public String key1;
    private long value;
    private long read_v;
    private boolean is_dead;
    private boolean update_succ;

    // for range query results
    private String key2;
    private Map<Long, Long> real_vals=new HashMap<>(); // ["k1:v1", "k2:v2"]
    private Map<Long, Long> dead_vals=new HashMap<>();

    // for string keys
    private Map<String, Long> vals=new HashMap<>();

    // {:type :ok, :f :txn, :value [[]]}
    private Map<OP_TYPE, String> op2template= new HashMap<OP_TYPE, String>() {{
        put(OP_TYPE.START_TXN, "[");
        put(OP_TYPE.READ, "[:r, %s, %d, %b]"); // [:r 30 3 true]
        put(OP_TYPE.WRITE, "[:w, %s, %d, %b]"); // [:w 18 2 true]
        put(OP_TYPE.COMMIT_TXN, ":type :ok");
        put(OP_TYPE.INSERT, "[:i %s %d %d %b %b]"); // [:i k v read_v is-dead succ]
        put(OP_TYPE.DELETE, "[:d, %s, %d, %d, %b, %b]"); // [:d k v read_v is-dead succ]
        put(OP_TYPE.RANGE, "[:range, %s, %s, %s, %s, %b]"); // [:range k1 k2 [{:id 2, :sk 2, :val 1}] [{:id 2, :sk 2, :val 1}]]
//        put(OP_TYPE.INSERT, "[:w %s %d %b]");
        put(OP_TYPE.FINAL, "[:range %s %s %s %b]");
//        put(OP_TYPE.DELETE, "[:d %s %s %s %s]");
//        put(OP_TYPE.ABORT_TXN, ":type :fail");
    }};

    public JepsenMop(OP_TYPE op, String key, long value, long read_v,
                     boolean is_dead, boolean update_succ, String key2){
        this.op_type = op;
//        this.txnid = txnid;
        this.key1 = key;
        this.key2 = key2;
        this.value = value;
        this.read_v = read_v;
        this.is_dead = is_dead;
        this.update_succ = update_succ;
    }

    public void setValuesArray(Map<Long, Long> real_vals, Map<Long, Long> dead_vals){
        this.real_vals = real_vals;
        this.dead_vals = dead_vals;
    }

    public void setFinalValues(Map<String, Long> vals){
        this.vals = vals;
    }

    private String values2String(Map<Long, Long> vals){
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

    public String toString(){
        String ret = null;
        boolean isFinal = false;

        switch (this.op_type){
            case READ:
                ret = String.format(op2template.get(this.op_type), this.key1, this.value, this.is_dead);
                break;
            case WRITE:
                ret = String.format(op2template.get(this.op_type), this.key1, this.value, this.update_succ);
                break;
            case INSERT:
            case DELETE:
                ret = String.format(op2template.get(this.op_type), this.key1, this.value, this.read_v,
                        this.is_dead, this.update_succ);
//                ret = String.format(op2template.get(this.op_type), this.key1, this.value, true);
                break;
//            case RANGE_INSERT:
//            case RANGE_DELETE:
//                ret = String.format(op2template.get(this.op_type), this.key1, this.value, this.read_v,
//                        this.is_dead, this.update_succ);
//                break;
            case RANGE:
                String real_vals_str = values2String(real_vals);
                String dead_vals_str = values2String(dead_vals);
                isFinal = false;
                if(this.key1 == null && this.key2 == null)
                    isFinal = true;
                ret = String.format(op2template.get(this.op_type), (this.key1 == null)? "nil": this.key1,
                        (this.key2 == null)? "nil": this.key2, real_vals_str, dead_vals_str, isFinal);
                break;
            case FINAL:
                String vals_str = values2String(real_vals);
                isFinal = true;

                ret = String.format(op2template.get(this.op_type), (this.key1 == null)? "nil": this.key1,
                        (this.key2 == null)? "nil": this.key2, vals_str, isFinal);
                break;
            default:
                System.out.println(String.format("Wrong mop: %s", ChengLogger.OP2NAME.get(this.op_type)));
                assert false;
                break;
        }

        return ret;
    }
}
