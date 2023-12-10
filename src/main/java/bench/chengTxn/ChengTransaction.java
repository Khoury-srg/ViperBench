package bench.chengTxn;

import bench.Transaction;
import bench.BenchUtils;
import bench.chengTxn.ChengTxnConstants.TASK_TYPE;
import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import main.Profiler;

public class ChengTransaction extends Transaction {
	private TASK_TYPE taskType;
	private int opsPerTxn;
	private String[] keys;
	private String[] keys2 = null;

	public ChengTransaction(KvInterface kvi, TASK_TYPE taskType, String[] keys, int opsPerTxn, boolean preBench) {
		super(kvi, getOpTag(taskType), preBench);
		this.taskType = taskType;
		this.opsPerTxn = opsPerTxn;
		this.keys = keys;

		this.txnName = ChengTxnConstants.TaskType2TxnTag.get(taskType);
	}

	public ChengTransaction(KvInterface kvi, TASK_TYPE taskType, boolean isInitialTxn, boolean isFinalTxn,
							boolean preBench){
		super(kvi, getOpTag(taskType), preBench);
		this.taskType = taskType;
		this.isInitialTxn = isInitialTxn;
		this.isFinalTxn = isFinalTxn;
	}

	/**
	 * range query requires */
	public void setKeysForRangeQuery(String[] keys2){
		this.keys2 = keys2;
	}

	@Override
	public void inputGeneration() {
		// do nothing
	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		Profiler.getInstance().startTick("kvi");
		beginTxn();
		Profiler.getInstance().endTick("kvi");

		if(isInitialTxn || isFinalTxn){ // global scan
			kvi.range(txn, null, null);
		} else {
			for (int i = 0; i < opsPerTxn; i++) {
				String key = keys[i];
				String key2;
				Profiler.getInstance().startTick("kvi");
				String val1 = "defaultValue";
				switch (taskType) { // For doBench(), call functions in InstKV
					case INSERT:
						val1 = BenchUtils.getRandomValue();
						kvi.insert(txn, key, val1);
						break;
					case UPDATE:
						val1 = BenchUtils.getRandomValue();
						kvi.set(txn, key, val1);
						break;
					case READ:
						kvi.get(txn, key);
						break;
					case READ_MODIFY_WRITE:
						String v = kvi.get(txn, key);
						v += "M";
						kvi.set(txn, key, v);
						i++;
						break;
					case DELETE:
						val1 = BenchUtils.getRandomValue();
						assert !preBench;
						kvi.delete(txn, key);
						break;
					case RANGE_QUERY: // Since our RocksDB KV interface doesn't support range query for now,
						// let's stop supporting range query here and switch to support YugaByteDB.
						key2 = keys2[i]; // range query requires another parameter
						assert !isInitialTxn && !isFinalTxn;
						kvi.range(txn, key, key2);
						break;
					case RANGE_VALUE_QUERY:
						val1 = BenchUtils.getRandomValue();
						// TODO
						break;
					default:
						assert false;
						break;
				}
				Profiler.getInstance().endTick("kvi");;
			}
		}

		Profiler.getInstance().startTick("kvi");
		commitTxn(isInitialTxn, isFinalTxn);
		Profiler.getInstance().endTick("kvi");;
		return true;
	}

	private static String getOpTag(TASK_TYPE taskType) {
		String tag = ChengTxnConstants.TaskType2TxnTag.get(taskType);
		return tag;
	}
}