package kv_interfaces.instrument;


import java.nio.ByteBuffer;
import java.util.Base64;

public class OpEncoder {
    public String val;
    public long txnid;
    public long wid;

    public OpEncoder(String val, long txnid, long wid) {
        this.val = val;
        this.txnid = txnid;
        this.wid = wid;
    }

    @Override
    public String toString() {
        return "val: " + val + ", txnid: " + txnid + ", wid: " + wid;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof OpEncoder) {
            OpEncoder that = (OpEncoder) obj;
            return this.val.equals(that.val) && this.txnid == that.txnid
                    && this.wid == that.wid;
        } else {
            return false;
        }
    }


    /**
     * @return a string of size 25 and ends with '&'
     */
    public static String encodeCobraValue(String val, long txnid, long wid) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
        buffer.putLong(txnid);
        buffer.putLong(wid);
        String str_sig = Base64.getEncoder().encodeToString(buffer.array());
        String val_sign = str_sig + val; // 24+140
        return "&"+val_sign; // to mark this thing is encoded
    }

    public static OpEncoder decodeCobraValue(String encoded_str) {
        try {
            if(encoded_str.length() < 25 || encoded_str.charAt(0) != '&') {
                return null;
            }
            String str_sig = encoded_str.substring(1, 25);
            String real_val = encoded_str.substring(25);
            byte[] barray = Base64.getDecoder().decode(str_sig);
            ByteBuffer bf = ByteBuffer.wrap(barray);
            long txnid = bf.getLong();
            long wid = bf.getLong();
            return new OpEncoder(real_val, txnid, wid);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String encodeDeadCobraInOneGo(String val, long txnid, long wid, boolean is_dead){
        String encoded_val = encodeCobraValue(val, txnid, wid);
        if(is_dead)
            encoded_val = DeadValueEncoder.encodeDeadValue(encoded_val);
        return encoded_val;
    }

//    public static void decodeDBValue(String dbVal, Object[] ret){
////        "[real_val, is_dead]"
//        assert ret.length == 2;
//        if(DeadValueEncoder.isDeadValue(dbVal))
//            ret[0] = DeadValueEncoder.decodeDeadValue(dbVal);
//
//
//    }
}

