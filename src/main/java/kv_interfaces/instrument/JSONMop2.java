package kv_interfaces.instrument;

import kv_interfaces.OP_TYPE;
import main.Config;

import java.util.HashMap;
import java.util.Map;


public class JSONMop2 extends JSONMop {
    private String tsTemplate =  ", \"reqTS\": %d, \"resTS\": %d";
    private Map<OP_TYPE, String> op2template= new HashMap<OP_TYPE, String>() {{
        String keyFormatStr = isIntegerKey? "%s": "\"%s\"";
        put(OP_TYPE.START_TXN, "");
        put(OP_TYPE.READ, "{\"opType\": \"r\", \"key\": " + keyFormatStr
                + ", \"value\": %d, \"isDead\": %b");
        // [:r 30 3 true]
        put(OP_TYPE.WRITE, "{\"opType\": \"w\", \"key\": "+ keyFormatStr +
                ", \"value\": %d, \"succ\": %b");
        // [:w 18 2
        // true]
        put(OP_TYPE.COMMIT_TXN, "{\"opType\": \"commit\" ");
        put(OP_TYPE.INSERT, "{\"opType\": \"i\", \"key\": " + keyFormatStr +", \"value\": %d, " +
                "\"readValue\": %d, \"isDead\": %b, \"succ\": %b");
        // [:i k v read_v is-dead succ]
        put(OP_TYPE.DELETE, "{\"opType\": \"d\", \"key\": " + keyFormatStr + ", \"value\": %d, \"readValue\": %d, " +
                "\"isDead\": %b, \"succ\": %b"); //
        // [:d k v read_v is-dead succ]
        put(OP_TYPE.RANGE, "{\"opType\": \"range\", \"key1\": %s, \"key2\": %s, " +
                "\"liveKVs\": %s, \"deadKVs\": %s"); //
        // [:range k1
        // k2 [{:id
        // 2, :sk 2,
        // :val 1}]
        // [{:id 2, :sk 2, :val 1}]]
    }};

    public JSONMop2(OP_TYPE op, String key, long value, long read_v,
                   boolean is_dead, boolean update_succ, String key2,
                    long req_timestamp, long res_timestamp){
        this.op_type = op;
//        this.txnid = txnid;
        this.key1 = key;
        this.key2 = key2;
        this.value = value;
        this.read_v = read_v;
        this.is_dead = is_dead;
        this.update_succ = update_succ;
        this.req_timestamp = req_timestamp;
        this.res_timestamp = res_timestamp;
    }

    public String toString(){
        String ret = null;

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

                String template = op2template.get(this.op_type);
                ret = String.format(template, (this.key1 == null)? Long.MIN_VALUE: this.key1,
                        (this.key2 == null)? Long.MAX_VALUE: this.key2, real_vals_str, dead_vals_str);
                break;
            case FINAL:
                assert false;
//                String vals_str = values2String(real_vals);
//                isInitial = true;
//
//                ret = String.format(op2template.get(this.op_type), (this.key1 == null)? "nil": this.key1,
//                        (this.key2 == null)? "nil": this.key2, vals_str, isInitial);
//                break;
            case START_TXN:
            case COMMIT_TXN:
                ret = String.format(op2template.get(this.op_type), this.req_timestamp, this.res_timestamp);
                break;
            default:
                System.out.println(String.format("Wrong mop: %s", ChengLogger.OP2NAME.get(this.op_type)));
                assert false;
                break;
        }
        if(Config.get().PRINT_TS)
            ret += String.format(tsTemplate, req_timestamp, res_timestamp);
//        if(isInitial || isFinal){
//            ret += String.format("isInitial: %b, isFinal %b", isInitial, isFinal);
//        }
        ret += "}";

        return ret;
    }
}