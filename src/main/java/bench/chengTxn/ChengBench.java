package bench.chengTxn;

import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import bench.Benchmark;
import bench.Transaction;
import bench.BenchUtils;
import bench.chengTxn.ChengTxnConstants.TASK_TYPE;
import kv_interfaces.KvInterface;
import main.Config;

public class ChengBench extends Benchmark {
	private static AtomicInteger keyNum;
	private final Random rand = new Random(Config.get().SEED);

	public ChengBench(KvInterface kvi) {
		super(kvi);
		keyNum = new AtomicInteger(Config.get().KEY_INDX_START);
		if(Config.get().SKIP_LOADING) {
			keyNum.addAndGet(Config.get().NUM_KEYS);
		}
	}

	@Override
	public Transaction[] preBenchmark() {
		// TODO need to clear the database

		// Fill in some keys
		int num_txn = (int) Math.ceil(((double) Config.get().NUM_KEYS) / Config.get().OP_PER_CHENGTXN);
//		int num_txn = 0;
		Transaction[] ret = new Transaction[num_txn];

		for (int i = 0; i < num_txn; i++) {
			ret[i] = getTheTxn(TASK_TYPE.INSERT, true);
		}
		return ret;
	}

	private TASK_TYPE nextOperation() {
		int dice = rand.nextInt(Config.get().RATIO_INSERT + Config.get().RATIO_READ +
				Config.get().RATIO_UPDATE + Config.get().RATIO_DELETE +
				Config.get().RATIO_RMW + Config.get().RATIO_RANGE + Config.get().RATIO_RANGE_VALUE);

		if (dice < Config.get().RATIO_INSERT) {
			return TASK_TYPE.INSERT;
		} else if (dice < Config.get().RATIO_INSERT + Config.get().RATIO_READ) {
			return TASK_TYPE.READ;
		} else if (dice < Config.get().RATIO_INSERT + Config.get().RATIO_READ +
				Config.get().RATIO_UPDATE) {
			return TASK_TYPE.UPDATE;
		} else if (dice < Config.get().RATIO_INSERT + Config.get().RATIO_READ +
				Config.get().RATIO_UPDATE + Config.get().RATIO_DELETE) {
			return TASK_TYPE.DELETE;
		} else if (dice < Config.get().RATIO_INSERT + Config.get().RATIO_READ
				+ Config.get().RATIO_UPDATE + Config.get().RATIO_DELETE + Config.get().RATIO_RMW){
			return TASK_TYPE.READ_MODIFY_WRITE;
		} else if(dice < Config.get().RATIO_INSERT + Config.get().RATIO_READ
				+ Config.get().RATIO_UPDATE + Config.get().RATIO_DELETE + Config.get().RATIO_RMW + Config.get().RATIO_RANGE) {
			return TASK_TYPE.RANGE_QUERY;
		} else {
			return TASK_TYPE.RANGE_VALUE_QUERY;
		}
	}

	/**
	 *
	 * @return a new key like "key9521"
	 * */
	private String getNewKey() {
		int cur_indx = keyNum.getAndIncrement();
		return Config.get().KEY_PRFIX + cur_indx;
	}

	/**
	 *
	 * @return an existing key between [KEY_INDX_START, keyNum)
	 * */
	private String getExistingKey() {
		int cur_indx = keyNum.get();
		if(Config.get().KEY_INDX_START == cur_indx)
			return Config.get().KEY_PRFIX + Config.get().KEY_INDX_START;
		return Config.get().KEY_PRFIX + BenchUtils.getRandomInt(Config.get().KEY_INDX_START, cur_indx);
	}

	private String[] getExistingKeys(int num) {
		HashSet<String> visited = new HashSet<>();
		String k;
		for (int i = 0; i < num; i++) {
			do {
				k = getExistingKey();
			} while (visited.contains(k));
			visited.add(k);
		}

		return visited.toArray(new String[0]);
	}

	private void swapKeys(String[] keys1, String[] keys2){
		assert keys1.length == keys2.length;
		long k1i, k2i;
		String tmp;

		for(int i = 0; i < keys1.length; i ++){
			k1i = Integer.valueOf(keys1[i]);
			k2i = Integer.valueOf(keys2[i]);
			if(k2i < k1i){
				tmp = keys1[i];
				keys1[i] = keys2[i];
				keys2[i] = tmp;
			}

			// DEBUG
			keys2[i] = (Integer.valueOf(keys1[i]) + 10) + "";
		}
	}

	public ChengTransaction getTheTxn(TASK_TYPE op, boolean preBench) {
		String keys1[] = null;
		String keys2[] = null;
		switch (op) {
			case INSERT:
				keys1 = new String[Config.get().OP_PER_CHENGTXN];
				for (int i = 0; i < keys1.length; i++) {
//				if(rand.nextDouble() < 0.5)
					keys1[i] = getNewKey();
//				else
//					keys1[i] = getExistingKey();
				}
				break;
//		case RANGE_INSERT: // only acquire new keys for INSERT
			case UPDATE: // use existing keys for all other cases
			case READ:
			case DELETE:
//		case RANGE_DELETE:
			case READ_MODIFY_WRITE:
				keys1 = getExistingKeys(Config.get().OP_PER_CHENGTXN);
				break;
			case RANGE_QUERY:
			case RANGE_VALUE_QUERY:
				keys1 = getExistingKeys(Config.get().OP_PER_CHENGTXN);
				keys2 = getExistingKeys(Config.get().OP_PER_CHENGTXN);
				swapKeys(keys1, keys2);
				break;
			default:
				assert false;
				System.exit(-1);
				break;
		}

		ChengTransaction nextTxn = new ChengTransaction(kvi, op, keys1, Config.get().OP_PER_CHENGTXN, preBench);
		if(op == TASK_TYPE.RANGE_QUERY || op == TASK_TYPE.RANGE_VALUE_QUERY){
			nextTxn.setKeysForRangeQuery(keys2);
		}

		return nextTxn;
	}

	@Override
	public Transaction getNextTxn() {
		TASK_TYPE op = nextOperation();
		return getTheTxn(op, false);
	}

	@Override
	public Transaction getFinalTransaction() {
		return new ChengTransaction(kvi, TASK_TYPE.RANGE_QUERY, false, true, false);
	}

	@Override
	public Transaction getInitialTransaction() {
		return new ChengTransaction(kvi, TASK_TYPE.RANGE_QUERY, true, false, false);
	}

	@Override
	public void afterBenchmark() {
		// TODO Auto-generated method stub
	}

	@Override
	public String[] getTags() {
		return new String[] { ChengTxnConstants.TXN_INSERT_TAG, ChengTxnConstants.TXN_DELETE_TAG,
				ChengTxnConstants.TXN_READ_TAG, ChengTxnConstants.TXN_RMW_TAG, ChengTxnConstants.TXN_UPDATE_TAG, "kvi"};
	}
}