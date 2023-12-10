package bench.ycsbt;

import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import bench.Benchmark;
import bench.Transaction;
import bench.chengTxn.ChengTransaction;
import bench.chengTxn.ChengTxnConstants;
import bench.ycsbt.YCSBTConstants.TASK_TYPE;
import kv_interfaces.KvInterface;
import main.Config;

public class YCSBTBench extends Benchmark {
	private AtomicInteger keyNum = new AtomicInteger(Config.get().KEY_INDX_START);
	private int totalCash = 1000;
	private Random rand = new Random(Config.get().SEED);

	public YCSBTBench(KvInterface kvi) {
		super(kvi);
	}

	@Override
	public Transaction[] preBenchmark() {
		// Fill in some keys
		int num_txn = Config.get().NUM_KEYS;
		Transaction ret[] = new Transaction[num_txn];
		for (int i = 0; i < num_txn; i++) {
			ret[i] = getTheTxn(TASK_TYPE.INSERT, true);
		}
		return ret;
	}

	private TASK_TYPE nextOperation() {
		int diceSum = Config.get().RATIO_INSERT + Config.get().RATIO_READ + Config.get().RATIO_UPDATE
				+ Config.get().RATIO_DELETE + Config.get().RATIO_RMW + Config.get().RATIO_RANGE;
		int dice = YCSBTUtils.RndIntRange(1, diceSum);
//		if (dice <= Config.get().RATIO_INSERT) {
//			return TASK_TYPE.INSERT;
//		} else if (dice <= Config.get().RATIO_INSERT + Config.get().RATIO_READ) {
//			return TASK_TYPE.READ;
//		} else if (dice <= Config.get().RATIO_INSERT + Config.get().RATIO_READ + Config.get().RATIO_UPDATE) {
//			return TASK_TYPE.UPDATE;
//		} else if (dice <= Config.get().RATIO_INSERT + Config.get().RATIO_READ + Config.get().RATIO_UPDATE
//				+ Config.get().RATIO_DELETE) {
//			return TASK_TYPE.DELETE;
//		} else if (dice <= Config.get().RATIO_INSERT + Config.get().RATIO_READ + Config.get().RATIO_UPDATE
//				+ Config.get().RATIO_DELETE + Config.get().RATIO_RMW){
//			return TASK_TYPE.READ_MODIFY_WRITE;
//		} else {
//			return TASK_TYPE.RANGE_QUERY;
//		}

		if (dice < Config.get().RATIO_INSERT) {
			return TASK_TYPE.INSERT;
		} else if (dice < Config.get().RATIO_INSERT + Config.get().RATIO_READ) {
			return TASK_TYPE.READ;
		} else if (dice < Config.get().RATIO_INSERT + Config.get().RATIO_READ + Config.get().RATIO_UPDATE) {
			return TASK_TYPE.UPDATE;
		} else if (dice < Config.get().RATIO_INSERT + Config.get().RATIO_READ + Config.get().RATIO_UPDATE
				+ Config.get().RATIO_DELETE) {
			return TASK_TYPE.DELETE;
		} else if (dice < Config.get().RATIO_INSERT + Config.get().RATIO_READ + Config.get().RATIO_UPDATE
				+ Config.get().RATIO_DELETE + Config.get().RATIO_RMW){
			return TASK_TYPE.READ_MODIFY_WRITE;
		} else {
			return TASK_TYPE.RANGE_QUERY;
		}

	}

	private String getNewKey() {
		int cur_indx = keyNum.getAndIncrement();
		return Config.get().KEY_PRFIX + cur_indx;
	}

	private String getExistingKey() {
		return Config.get().KEY_PRFIX + (Config.get().KEY_INDX_START + YCSBTUtils.zipfian());
	}

	private String[] getExistingKeys(int num) {
		HashSet<String> visited = new HashSet<>();
		String k = null;
		for (int i = 0; i < num; i++) {
			do {
				k = getExistingKey();
			} while (visited.contains(k));
			visited.add(k);
		}
		return visited.toArray(new String[0]);
	}

//	private void swapKeys(String[] keys1, String[] keys2){
//		assert keys1.length == keys2.length;
//		long k1i, k2i;
//		String tmp = null;
//
//		for(int i = 0; i < keys1.length; i ++){
//			k1i = Integer.valueOf(keys1[i].substring(3));
//			k2i = Integer.valueOf(keys2[i].substring(3));
//			if(k2i < k1i){
//				tmp = keys1[i];
//				keys1[i] = keys2[i];
//				keys2[i] = tmp;
//			}
//		}
//		return;
//	}

	private Transaction getTheTxn(TASK_TYPE op, boolean preBench) {
		String keys1[] = null;

		switch (op) {
			case INSERT:
				keys1 = new String[Config.get().OP_PER_CHENGTXN];
				for (int i = 0; i < keys1.length; i++) {
//					if (rand.nextDouble() < 0.5)
					keys1[i] = getNewKey();
//					else
//						keys1[i] = getExistingKey();
				}
				break;
//			case RANGE_INSERT: // only acquire new keys for INSERT
//				keys1 = new String[Config.get().OP_PER_CHENGTXN];
//				for (int i = 0; i < keys1.length; i++) {
////					if (rand.nextDouble() < 0.5)
//						keys1[i] = getNewKey();
////					else
////						keys1[i] = getExistingKey();
//				}
//				break;
			case UPDATE:
			case READ:
			case DELETE:
//			case RANGE_DELETE:
				keys1 = new String[1];
				keys1[0] = getExistingKey();
				break;
			case READ_MODIFY_WRITE:
			case RANGE_QUERY:
				keys1 = new String[2];
				keys1[0] = getExistingKey(); // actually range query can use new keys, RMW can only use existing keys.
				keys1[1] = getExistingKey();
				break;
			case SCAN:
				assert false; // I think YCSNBench currently doesn't support SCAN, why has this branch?
				keys1 = new String[10];
				for (int i = 0; i < keys1.length; i++) {
					keys1[i] = getExistingKey();
				}
				break;
			default:
				assert false;
				break;
		}
		YCSBTTransaction nextTxn = new YCSBTTransaction(kvi, op, keys1, preBench);
//		if(op == TASK_TYPE.RANGE_QUERY || op == TASK_TYPE.READ_MODIFY_WRITE){
//			nextTxn.setKeysForRangeQuery(keys2);
//		}
		return nextTxn;
	}

	@Override
	public Transaction getNextTxn() {
		TASK_TYPE op = nextOperation();
		return getTheTxn(op, false);
	}

	@Override
	public Transaction getFinalTransaction() {
		YCSBTTransaction txn = new YCSBTTransaction(kvi,
				TASK_TYPE.RANGE_QUERY, true, false);
		return txn;
	}

	@Override
	public Transaction getInitialTransaction() {
		return null;
	}

	@Override
	public void afterBenchmark() {
		assert false;
		// TODO Auto-generated method stub

	}

	@Override
	public String[] getTags() {
		return new String[] { YCSBTConstants.TXN_READ_TAG, YCSBTConstants.TXN_INSERT_TAG, YCSBTConstants.TXN_RMW_TAG,
				YCSBTConstants.TXN_UPDATE_TAG, YCSBTConstants.TXN_SCAN_TAG, YCSBTConstants.TXN_DELETE_TAG };
	}

}
