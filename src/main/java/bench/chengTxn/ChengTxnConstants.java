package bench.chengTxn;

import java.util.Map;

public class ChengTxnConstants {
	// task type
	enum TASK_TYPE {
		INSERT, RANGE_INSERT, READ, UPDATE, DELETE, RANGE_DELETE, READ_MODIFY_WRITE, RANGE_QUERY, RANGE_VALUE_QUERY
	}

	// profiling related
	public final static String TXN_READ_TAG = "txnread";
	public final static String TXN_INSERT_TAG = "txninsert";
	public final static String TXN_UPDATE_TAG = "txnupdate";
	public final static String TXN_DELETE_TAG = "txndelete";
	public final static String TXN_RMW_TAG = "txnrmw";
	public final static String TXN_RANGE_TAG = "txnrange";
	public final static String TXN_RANGE_VALUE_TAG = "txnrangevalue";
	public final static Map<TASK_TYPE, String> TaskType2TxnTag = Map.of(
			TASK_TYPE.INSERT, TXN_INSERT_TAG,
			TASK_TYPE.READ, TXN_READ_TAG,
			TASK_TYPE.UPDATE, TXN_UPDATE_TAG,
			TASK_TYPE.DELETE, TXN_DELETE_TAG,
			TASK_TYPE.READ_MODIFY_WRITE, TXN_RMW_TAG,
			TASK_TYPE.RANGE_QUERY, TXN_RANGE_TAG,
			TASK_TYPE.RANGE_VALUE_QUERY, TXN_RANGE_VALUE_TAG
			);
}