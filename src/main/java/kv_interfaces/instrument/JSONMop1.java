package kv_interfaces.instrument;

import kv_interfaces.OP_TYPE;
import main.Config;

import java.util.HashMap;
import java.util.Map;


public class JSONMop1 extends JSONMop {

    private Map<OP_TYPE, String> op2template= new HashMap<OP_TYPE, String>() {{
        put(OP_TYPE.START_TXN, "start");
        put(OP_TYPE.READ, isIntegerKey? "[\"r\", %s, %d, %b]": "[\"r\", \"%s\", %d, %b]"); // [:r 30 3 true]
        put(OP_TYPE.WRITE, isIntegerKey? "[\"w\", %s, %d, %b]": "[\"w\", \"%s\", %d, %b]"); // [:w 18 2 true]
        put(OP_TYPE.COMMIT_TXN, "commit");
        put(OP_TYPE.INSERT, isIntegerKey? "[\"i\", %s, %d, %d, %b, %b]":
                "[\"i\", \"%s\", %d, %d, %b, %b]");
        // [:i k v read_v is-dead succ]
        put(OP_TYPE.DELETE, isIntegerKey? "[\"d\", %s, %d, %d, %b, %b]":
                "[\"d\", \"%s\", %d, %d, %b, %b]"); //
        // [:d k v read_v is-dead succ]
        put(OP_TYPE.RANGE, "[\"range\", %s, %s, %s, %s]"); // [:range k1 k2 [{:id 2, :sk 2, :val 1}] [{:id 2, :sk 2, :val 1}]]
    }};

    public JSONMop1(OP_TYPE op, String key, long value, long read_v,
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
                String template = op2template.get(this.op_type);

                ret = String.format(template, (this.key1 == null)? "\"nil\"": this.key1,
                        (this.key2 == null)? "\"nil\"": this.key2, real_vals_str, dead_vals_str);
                break;
            case START_TXN:
            case COMMIT_TXN:
                ret = op2template.get(this.op_type);
                break;
            default:
                System.out.println(String.format("Wrong mop: %s", ChengLogger.OP2NAME.get(this.op_type)));
                assert false;
                break;
        }

        return ret;
    }

}