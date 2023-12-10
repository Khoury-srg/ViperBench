package main;

import java.sql.Timestamp;

public class MyTimestamp {

    /**
     * @return current timestamp in milliseconds
     */
    public static long getTimestamp(){
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        return timestamp.getTime();
    }
}
