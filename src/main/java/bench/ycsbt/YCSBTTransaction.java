package bench.ycsbt;

import bench.Transaction;
import bench.BenchUtils;
import bench.ycsbt.YCSBTConstants.TASK_TYPE;
import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import main.Logger;

public class YCSBTTransaction extends Transaction {

	private TASK_TYPE taskType;
	private String keys[];

	public YCSBTTransaction(KvInterface kvi, TASK_TYPE taskType, String keys[], boolean preBench) {
		super(kvi, getOpTag(taskType), preBench);
		this.taskType = taskType;
		this.keys = keys;
	}

	public YCSBTTransaction(KvInterface kvi, TASK_TYPE taskType, boolean isFinalTxn, boolean preBench){
		super(kvi, getOpTag(taskType), preBench);
		this.isFinalTxn = isFinalTxn;
	}

	@Override
	public void inputGeneration() {
		// do nothing
	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		beginTxn();

		if(isInitialTxn || isFinalTxn){
			kvi.range(txn, null, null);
		} else {
			int key_counter = 0;
			String key = keys[key_counter++];
			String val = "defaultValue";
			String key2 = null;
			switch (taskType) {
				case INSERT:
					val = BenchUtils.getRandomValue();
					kvi.insert(txn, key, val);
//					if(preBench)
//						kvi.insert(txn, key, val);
//					else
//						kvi.rangeInsert(txn, key, val);
					break;
//				case RANGE_INSERT:
//					val = BenchUtils.getRandomValue();
//					if(preBench)
//						kvi.insert(txn, key, val);
//					else
//						kvi.rangeInsert(txn, key, val);
//					break;
				case UPDATE:
					val = BenchUtils.getRandomValue();
					// TODO: can I do this? why shuffle? it can be null value, existing key doesn't necessarily in the DB
//				String v = kvi.get(txn, key);
//				v = BenchUtils.shuffleString(v);
					kvi.set(txn, key, val);
					break;
				case READ:
					kvi.get(txn, key);
					break;
				case READ_MODIFY_WRITE:
					key2 = keys[key_counter++];
					String v1 = kvi.get(txn, key);
					String v2 = kvi.get(txn, key2);
					kvi.set(txn, key, v2);
					kvi.set(txn, key2, v1);
					break;
				case DELETE:
					val = BenchUtils.getRandomValue();
					kvi.delete(txn, key); // since DB interface doesn't have rangeDelete
//					if(preBench)
//						kvi.delete(txn, key); // since DB interface doesn't have rangeDelete
//					else
//						kvi.rangeDelete(txn, key, val);
					break;
//				case RANGE_DELETE:
//					val = BenchUtils.getRandomValue();
//					if(preBench)
//						kvi.delete(txn, key); // since DB interface doesn't have rangeDelete
//					else
//						kvi.rangeDelete(txn, key, val);
//					break;
				case RANGE_QUERY: // Since our RocksDB KV interface doesn't support range query for now,
					// let's stop supporting range query here and switch to support YugaByteDB.
					key2 = keys[key_counter++]; // range query requires another parameter
					assert !isInitialTxn && !isFinalTxn;
					kvi.range(txn, key, key2);
					break;
				default:
					assert false;
					break;
			}
		}

		commitTxn(isInitialTxn, isFinalTxn);
		return true;
	}

	private static String getOpTag(TASK_TYPE op) {
		String tag = "";
		switch (op) {
		case INSERT:
			tag = YCSBTConstants.TXN_INSERT_TAG;
			break;
		case READ:
			tag = YCSBTConstants.TXN_READ_TAG;
			break;
		case UPDATE:
			tag = YCSBTConstants.TXN_UPDATE_TAG;
			break;
		case DELETE:
			tag = YCSBTConstants.TXN_DELETE_TAG;
			break;
		case READ_MODIFY_WRITE:
			tag = YCSBTConstants.TXN_RMW_TAG;
			break;
		case SCAN:
			assert false;
			tag = YCSBTConstants.TXN_SCAN_TAG;
			break;
		default:
			tag = "unkonwn";
			Logger.logError("UNKOWN TASK_TYPE[" + op + "]");
			break;
		}
		return tag;
	}
}
