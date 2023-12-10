package bench.simpleSQL;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;

public class SimpleSQLConstants {
    public final static String table = "simplesql";

    public enum TASK_TYPE {
        INSERT, DELETE, UPDATE, SELECT
    }

    /**
     * %s: table name
     * %d: written value, need to respect uniqueness
     * ?: other regular parameters
     */
    public final static Map<TASK_TYPE, String> sqlTemplates = Map.of(
            TASK_TYPE.INSERT, "INSERT INTO %s VALUES ( ? , %d , %d )", // regular: 1; written values: 2
            TASK_TYPE.DELETE, "DELETE FROM %s WHERE id = ?", // regular 1; written values: 0
            TASK_TYPE.SELECT, "SELECT v2 FROM %s WHERE v1 >= ? AND id <= ?", // regular: 2, written values: 0
            TASK_TYPE.UPDATE, "UPDATE %s SET v2 = %d WHERE v1 >= ?"); // regular: 1, written values: 1

    public final static Map<TASK_TYPE, Pair<Integer, Integer>> numArgs = Map.of(
        TASK_TYPE.INSERT, Pair.of(1,2),
        TASK_TYPE.DELETE, Pair.of(1, 0),
        TASK_TYPE.SELECT, Pair.of(2, 0),
        TASK_TYPE.UPDATE, Pair.of(1,1)
    );

    public final static String TXN_SELECT_TAG = "txnselect";
    public final static String TXN_INSERT_TAG = "txninsert";
    public final static String TXN_UPDATE_TAG = "txnupdate";
    public final static String TXN_DELETE_TAG = "txndelete";

    public static final Map<TASK_TYPE, String> TaskType2TxnTag = Map.of(
            TASK_TYPE.INSERT, TXN_INSERT_TAG,
            TASK_TYPE.SELECT, TXN_SELECT_TAG,
            TASK_TYPE.UPDATE, TXN_UPDATE_TAG,
            TASK_TYPE.DELETE, TXN_DELETE_TAG);
}
