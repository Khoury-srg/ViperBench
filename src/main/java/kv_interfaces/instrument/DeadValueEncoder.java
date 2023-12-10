package kv_interfaces.instrument;

public class DeadValueEncoder {
    public static String encodeDeadValue(String s){
        return '|'+ s;
    }

    public static String decodeDeadValue(String s){
        assert s.startsWith("|");
        return s.substring(1);
    }

    public static boolean isDeadValue(String s){
        return s != null && s.startsWith("|");
    }


}
