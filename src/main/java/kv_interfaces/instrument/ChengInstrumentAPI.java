package kv_interfaces.instrument;

import java.util.Map;

import bench.simpleSQL.SimpleSQLConstants;
import kv_interfaces.KvInterface;
import kvstore.exceptions.TxnException;
import main.Config;
import net.openhft.hashing.LongHashFunction;

public class ChengInstrumentAPI {
	public static void main(String[] args) {
		String val = "asdasas";
		long txnid = 1222313;
		long wid = 0xdeadbeefL;
		String encoded_str = OpEncoder.encodeCobraValue(val, txnid, wid);
		long t1 = System.currentTimeMillis();
		for (int i = 0; i < 1600000; i++) {
			OpEncoder.decodeCobraValue(encoded_str);
		}
		long t2 = System.currentTimeMillis();
		System.out.println(t2 - t1);
	}

	public enum WRITE_TYPE {
		INSERT, UPDATE, DELETE, RANGE_INSERT, RANGE_DELETE
	}

	// ==========================
	// === Begin/Commit/Abort ===
	// ==========================

	public static void doTransactionBegin(long txnid) {
		ChengClientState.initTxnId(txnid);
		ChengLogger logger = ChengLogger.getInstance();
		logger.txnStart(txnid);
		logger.debug("Txn[" + Long.toHexString(txnid) + "] start");

		AllLogger allLogger = AllLogger.getInstance();
		allLogger.txnStart(txnid);
		// JsonLogger jlogger = JsonLogger.getInstance();
		// jlogger.txnStart(txnid);
		assert ChengClientState.getInstance().r_set.size() == 0;
	}

	public static LogObject doTransactionCommitPre(KvInterface kvi, long txnid,
			long request_timestamp)
			throws TxnException {
		ChengLogger logger = ChengLogger.getInstance();

		// Cobra's binary logs
		logger.txnCommitPre(txnid); // add the COMMIT to byte array
		logger.debug("Txn[" + Long.toHexString(txnid) + "] try to commit");

		// Viper's json and jepsen logs
		AllLogger allLogger = AllLogger.getInstance();
		allLogger.txnCommitPre(txnid, request_timestamp);
		// JsonLogger jlogger = JsonLogger.getInstance();
		// jlogger.txnCommitPre(txnid);
		// if online log, construct the log entity
		if (Config.get().CLOUD_LOG) {
			// (1) update the opLogBuffer to log object
			// (2) return the log object
			LogObject lobj = ChengClientState.append2logobj(logger.getOpLogBuffer());
			return lobj;
		}
		return null;
	}

	public static void doTransactionCommitPost(long txnid,
			long res_timestamp, boolean isInitial, boolean isFinal) {
		ChengLogger logger = ChengLogger.getInstance();
		logger.txnCommitPost(txnid);

		AllLogger allLogger = AllLogger.getInstance();
		allLogger.txnCommitPost(txnid, res_timestamp, isInitial, isFinal);
		// JsonLogger jlogger = JsonLogger.getInstance();
		// jlogger.txnCommitPost(txnid);

		ChengClientState.getInstance().epochTxnNum++;
		logger.debug("Txn[" + Long.toHexString(txnid) + "] successfully commit");

		ChengClientState.removeTxnId();
		// if Cloud_LOG, need to move forward the entity and hash
		if (Config.get().CLOUD_LOG) {
			// txn committed successfully, so we can safely moving forward by
			// setting the clog_hash/wo_hash and entity
			ChengClientState cs = ChengClientState.getInstance();
			if (cs == null)
				return;
			cs.successCommitOneEntity();
		}

		ChengClientState.getInstance().r_set.clear();
	}

	// abort might happen outside txn
	public static void doTransactionRollback(long txnid) {
		if (!ChengClientState.inTxn())
			return;

		ChengLogger logger = ChengLogger.getInstance();
		logger.txnAbort(txnid);
		logger.debug("Txn[" + Long.toHexString(txnid) + "] abort");

		AllLogger allLogger = AllLogger.getInstance();
		allLogger.txnAbort(txnid);
		// JsonLogger jlogger = JsonLogger.getInstance();
		// jlogger.txnAbort(txnid);

		ChengClientState.removeTxnId();
		// if Cloud_LOG, need to roll-back the entity and hash
		if (Config.get().CLOUD_LOG) {
			ChengClientState cs = ChengClientState.getInstance();
			if (cs == null)
				return;
			cs.rollbackLogObj();
		}

		ChengClientState.getInstance().r_set.clear();
	}

	public static class DoWriteReturn {
		// public EncodedStr_Hash(String encoded_str, long val_hash){
		// this.encoded_str = encoded_str;
		// this.val_hash = val_hash;
		// }

		public DoWriteReturn(String encoded_str, long val_hash,
				long key_hash,
				long wid) {
			this.encoded_str = encoded_str;
			this.key_hash = key_hash;
			this.val_hash = val_hash;
			this.wid = wid;
		}

		public long key_hash;
		public long wid;
		public long val_hash;
		public String encoded_str;
	}

	public static class DoReadReturn {
		public DoReadReturn(String real_val, long val_hash, long write_txnid,
				long wid, long key_hash) {
			this.real_val = real_val;
			this.key_hash = key_hash;
			this.val_hash = val_hash;
			this.write_txnid = write_txnid;
			this.wid = wid;
		}

		public String real_val;
		public long key_hash;
		public long val_hash;
		public long write_txnid;
		public long wid;
	}

	public static void doTransactionRangeQuery(long txnid, String key1, String key2,
			Map<Long, Long> real_vals,
			Map<Long, Long> dead_vals,
			long req_timestamp,
			long res_timestamp) {
		// compute hashed values
		// Map<Long, Long> m1 = hashArrays(real_vals, txnid);
		// Map<Long, Long> m2 = hashArrays(real_vals, txnid);

		AllLogger allLogger = AllLogger.getInstance();
		allLogger.txnRangeQuery(key1, key2, real_vals, dead_vals, req_timestamp,
				res_timestamp);
		// JsonLogger jlogger = JsonLogger.getInstance();
		// jlogger.txnRangeQuery(key1, key2, real_vals, dead_vals);
	}

	// public static void doTransactionFinalState(long txnId, Map<String, Long>
	// val_hashes) {
	// JepsenLogger jlogger = JepsenLogger.getInstance();
	// jlogger.txnFinalState(val_hashes);
	// }

	public static DoWriteReturn hashValue(long txnid, String key, String val) {
		// 1. generate a write id
		// if ADD/UPDATE/PUT, generate a new id
		// if DELETE, use NOP_WRITE_ID (because the next INSERT/PUT will use
		// NOP_WRITE_ID as prev_write)
		// FIXME: this might be a problem here for the delete.
		// long wid = (type == WRITE_TYPE.DELETE) ? LibConstants.DELETE_WRITE_ID :
		// ChengIdGenerator.genWriteId();
		long wid = ChengIdGenerator.genWriteId();

		// 2. encode a value (key, txnid, wid) for next read
		String encoded_val = OpEncoder.encodeCobraValue(val, txnid, wid);

		// 3. record the client log
		long key_hash = LongHashFunction.xx().hashChars(key);
		long val_hash = LongHashFunction.xx().hashChars(encoded_val);

		DoWriteReturn str_hash = new DoWriteReturn(encoded_val, val_hash,
				key_hash, wid);

		return str_hash;
	}

	public static void doWrite(WRITE_TYPE type, long txnid, String key, long key_hash,
			long val_hash, long write_id, long read_val_hash,
			boolean is_dead, boolean log, boolean succ,
			long req_timestamp, long res_timestamp) throws TxnException {
		assert ChengClientState.inTxn();

		if (log) {
			// Cobra logs only hash values of keys and encoded values
			// ChengLogger.getInstance().txnWrite(txnid, wid, key_hash, val_hash);
			// JepsenLogger.getInstance().txnWrite(key, val_hash);
			// JsonLogger Jlogger = JsonLogger.getInstance();
			AllLogger allLogger = AllLogger.getInstance();

			ChengLogger logger = ChengLogger.getInstance();
			switch (type) {
				case INSERT:
				case UPDATE:
					assert succ;
					logger.txnWrite(txnid, write_id, key_hash, val_hash);
					allLogger.txnWrite(key, val_hash, succ, req_timestamp, res_timestamp);
					break;
				case DELETE:
					allLogger.txnDelete(key, val_hash, read_val_hash, is_dead,
							succ, req_timestamp, res_timestamp);
					break;
				default:
					assert false;
			}
		}
	}

	public static DoReadReturn deHash(long txnid, String key, String dbVal) {
		// do I allow val to be null? Yes
		assert txnid != 0;
		final String real_val;
		final long write_txnid, write_id, key_hash, value_hash;
		ChengLogger logger = ChengLogger.getInstance();

		if (dbVal != null) {
			OpEncoder op = OpEncoder.decodeCobraValue(dbVal);
			if (op == null) {
				// Tricky part: The values written in initialization period are not encoded, so
				// we mark it
				write_txnid = LibConstants.INIT_TXN_ID;
				write_id = LibConstants.INIT_WRITE_ID;
				real_val = dbVal;
			} else {
				write_txnid = op.txnid;
				write_id = op.wid;
				real_val = op.val;
			}
			value_hash = LongHashFunction.xx().hashChars(dbVal);
		} else {
			write_txnid = LibConstants.NULL_TXN_ID;
			write_id = LibConstants.NULL_WRITE_ID;
			// Read nothing, might be reading a non-existing key
			logger.error("Signature is null for key [" + key + "]");
			real_val = null;
			value_hash = 0; // NOTE: this is the value hash for "null" value
		}

		key_hash = LongHashFunction.xx().hashChars(key);
		return new DoReadReturn(real_val, value_hash, write_txnid, write_id, key_hash);
	}

	/**
	 * dbVal is directly from the DB, but if it is dead, it has been already decoded
	 * from the dead value.
	 * 
	 * @return Return decoded value if the input is encoded; return null if there is
	 *         any decode error.
	 */
	public static void doRead(long txnid, String key, long key_hash, long val_hash,
			long write_txnid, long wid, boolean is_dead,
			long req_timestamp, long res_timestamp) {
		ChengLogger logger = ChengLogger.getInstance();
		// JsonLogger jlogger = JsonLogger.getInstance();
		AllLogger allLogger = AllLogger.getInstance();
		ChengClientState.getInstance().r_set.add(key);

		logger.txnRead(txnid, write_txnid,
				wid, key_hash, val_hash);
		allLogger.txnRead(key, val_hash, is_dead, req_timestamp, res_timestamp);
	}

	public static void doSQL(String sql, SimpleSQLConstants.TASK_TYPE taskType, Object[] ret){
		AllLogger allLogger = AllLogger.getInstance();

		switch(taskType){
			case INSERT:
				allLogger.txnInsert2(sql);
				break;
			case DELETE:
				allLogger.txnDelete2(sql);
				break;
			case SELECT:
				allLogger.txnSelect2(sql, ret);
				break;
			case UPDATE:
				allLogger.txnUpdate2(sql, -1);
				break;
			default:
				assert false;
		}
	}
}