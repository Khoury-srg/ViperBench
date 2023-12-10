package kv_interfaces;

import bench.BenchUtils;
import bench.simpleSQL.SimpleSQLConstants;
import kv_interfaces.instrument.*;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import main.Config;
import main.MyTimestamp;

import java.util.HashMap;
import java.util.Map;

// a wrapper for real KvInterface (the "kvi")
public class InstKV implements KvInterface {
	public final KvInterface kvi;

	public InstKV(KvInterface orig_kvi) {
		this.kvi = orig_kvi;
		assert !kvi.isInstrumented(); // prevent nested instrument
	}

	@Override
	public Object begin() throws KvException, TxnException {
		Object txn = kvi.begin();
		long txnid = kvi.getTxnId(txn);
		ChengInstrumentAPI.doTransactionBegin(txnid);
		return txn;
	}

	@Override
	public boolean commit(Object txn, boolean isInitial, boolean isFinal) throws KvException, TxnException {
		return commit(txn, true, isInitial, isFinal);
	}

	public boolean commit(Object txn, boolean updateEpoch, boolean isInitial, boolean isFinal) throws KvException,
			TxnException {
		long txnid = kvi.getTxnId(txn);
		ChengClientState status = ChengClientState.getInstance();

		// NOTE: commit might fail; we simply cache the read result
		// UTBABUG: much do read before CommitPre()
		long req_timestamp = MyTimestamp.getTimestamp();
		LogObject lo = ChengInstrumentAPI.doTransactionCommitPre(kvi, txnid,
				req_timestamp);

		if (Config.get().CLOUD_LOG) {
			assert lo != null;
			String clkey = lo.getCLkey();
			String clval = lo.getCLentry();
			boolean success = kvi.set(txn, clkey, clval);
			assert success; // there should be no contention on this
		}

		boolean ret = kvi.commit(txn, isInitial, isFinal);

		// [cheng: ??? shouldn't we check "ret"? what if commit fails?
		assert ret;
		long res_timestamp = MyTimestamp.getTimestamp();
		ChengInstrumentAPI.doTransactionCommitPost(txnid, res_timestamp, isInitial, isFinal);

		return ret;
	}

	@Override
	public boolean abort(Object txn) throws KvException, TxnException {
		boolean ret = kvi.abort(txn);
		ChengInstrumentAPI.doTransactionRollback(kvi.getTxnId(txn));
		return ret;
	}

	@Override
	public boolean insert(Object txn, String key, String write_value) throws KvException, TxnException {
		// // [encoded_str, val_hash]
		// ChengInstrumentAPI.DoWriteReturn encodedStr_hash =
		// ChengInstrumentAPI.hashValue(kvi.getTxnId(txn), key, real_val);
		//
		// boolean ret = kvi.insert(txn, key, encodedStr_hash.encoded_str);
		// ChengInstrumentAPI.doWrite(ChengInstrumentAPI.WRITE_TYPE.INSERT,
		// kvi.getTxnId(txn),
		// key, encodedStr_hash.key_hash,
		// encodedStr_hash.val_hash,
		// encodedStr_hash.wid, -1, false, true, ret);
		// return ret;
		// read
		// String read_val = kvi.get(txn, key);
		// boolean is_dead = DeadValueEncoder.isDeadValue(read_val);
		//
		// if(read_val != null && !is_dead)
		// throw new TxnException("Can't insert an existing key");

		// not exist or a dead value
		// if(is_dead)
		// read_val = DeadValueEncoder.decodeDeadValue(read_val);

		// ChengInstrumentAPI.DoReadReturn read_EncodedStr_hash =
		// ChengInstrumentAPI.deHash(kvi.getTxnId(txn), key, read_val);

		// insert
		ChengInstrumentAPI.DoWriteReturn write_EncodedStr_hash = ChengInstrumentAPI.hashValue(kvi.getTxnId(txn),
				key, write_value);

		// boolean succ = false;
		// if(is_dead)
		// succ = kvi.set(txn, key, write_EncodedStr_hash.encoded_str);
		// else
		long req_timestamp = MyTimestamp.getTimestamp();
		boolean succ = kvi.insert(txn, key, write_EncodedStr_hash.encoded_str);
		long res_timestamp = MyTimestamp.getTimestamp();
		ChengInstrumentAPI.doWrite(ChengInstrumentAPI.WRITE_TYPE.UPDATE,
				kvi.getTxnId(txn), key, write_EncodedStr_hash.key_hash,
				write_EncodedStr_hash.val_hash, write_EncodedStr_hash.wid,
				-1, false, true, succ, req_timestamp,
				res_timestamp);

		return succ;
	}

	@Override
	public boolean delete(Object txn, String key) throws KvException, TxnException {
		// read
		String read_val = kvi.get(txn, key); // if not exist, is it null?

		boolean is_dead = DeadValueEncoder.isDeadValue(read_val);
		if (read_val == null || is_dead)
			throw new TxnException("Cannot delete a non-existing key");

		// exists and not dead value
		ChengInstrumentAPI.DoReadReturn read_EncodedStr_hash = ChengInstrumentAPI.deHash(kvi.getTxnId(txn), key,
				read_val);

		// delete
		String write_value = BenchUtils.getRandomValue();

		ChengInstrumentAPI.DoWriteReturn write_EncodedStr_hash = ChengInstrumentAPI.hashValue(kvi.getTxnId(txn),
				key, write_value);

		String dead_value = DeadValueEncoder.encodeDeadValue(write_EncodedStr_hash.encoded_str);
		long req_timestamp = MyTimestamp.getTimestamp();
		boolean succ = kvi.set(txn, key, dead_value);
		long res_timestamp = MyTimestamp.getTimestamp();
		ChengInstrumentAPI.doWrite(ChengInstrumentAPI.WRITE_TYPE.DELETE,
				kvi.getTxnId(txn), key, write_EncodedStr_hash.key_hash,
				write_EncodedStr_hash.val_hash, write_EncodedStr_hash.wid,
				read_EncodedStr_hash.val_hash, is_dead, true, succ,
				req_timestamp, res_timestamp);

		return succ;
	}

	// @Override
	// public boolean rangeDelete(Object txn, String key, String write_value) throws
	// KvException, TxnException {
	//
	// }

	@Override
	public String get(Object txn, String key) throws KvException, TxnException {
		long req_timestamp = MyTimestamp.getTimestamp();
		String read_val = kvi.get(txn, key);
		long res_timestamp = MyTimestamp.getTimestamp();
		boolean is_dead = DeadValueEncoder.isDeadValue(read_val);
		if (is_dead)
			read_val = DeadValueEncoder.decodeDeadValue(read_val);

		// Map<String, String> map = Tables.DecodeTableKey(key, new
		// String[]{WriteSkewBench.TABLE_A});
		// String real_val = ChengInstrumentAPI.doRead(kvi.getTxnId(txn),
		// map.get("key"), val, true, true);
		// exists and not dead value
		ChengInstrumentAPI.DoReadReturn encodedStr_hash = ChengInstrumentAPI.deHash(kvi.getTxnId(txn), key, read_val);
		ChengInstrumentAPI.doRead(kvi.getTxnId(txn), key, encodedStr_hash.key_hash,
				encodedStr_hash.val_hash, encodedStr_hash.write_txnid,
				encodedStr_hash.wid, is_dead, req_timestamp, res_timestamp);

		return encodedStr_hash.real_val;
	}

	@Override
	public boolean set(Object txn, String key, String write_value) throws KvException, TxnException {
		ChengInstrumentAPI.DoWriteReturn encoded_struct = ChengInstrumentAPI.hashValue(kvi.getTxnId(txn),
				key, write_value);

		long req_timestamp = MyTimestamp.getTimestamp();
		boolean succ = kvi.set(txn, key, encoded_struct.encoded_str);
		long res_timestamp = MyTimestamp.getTimestamp();
		ChengInstrumentAPI.doWrite(ChengInstrumentAPI.WRITE_TYPE.UPDATE,
				kvi.getTxnId(txn), key, encoded_struct.key_hash,
				encoded_struct.val_hash, encoded_struct.wid,
				-1, false, true, succ, req_timestamp,
				res_timestamp);

		return succ;
	}

	@Override
	public Map<Long, String> range(Object txn, String key1, String key2) throws KvException,
			TxnException {
		long req_timestamp = MyTimestamp.getTimestamp();
		Map<Long, String> vals = kvi.range(txn, key1, key2);
		long res_timestamp = MyTimestamp.getTimestamp();
		Map<Long, String> real_vals = new HashMap<>();
		Map<Long, Long> real_val_hashes = new HashMap<>();
		Map<Long, Long> dead_vals_hashes = new HashMap<>();

		String curr_val = null;
		for (Map.Entry<Long, String> entry : vals.entrySet()) {
			curr_val = entry.getValue();

			if (DeadValueEncoder.isDeadValue(entry.getValue())) {
				curr_val = DeadValueEncoder.decodeDeadValue(curr_val);

				ChengInstrumentAPI.DoReadReturn encodedStr_hash = ChengInstrumentAPI.deHash(kvi.getTxnId(txn),
						entry.getKey() + "", curr_val);
				long read_val_hash = encodedStr_hash.val_hash;
				String real_val = encodedStr_hash.real_val;

				dead_vals_hashes.put(entry.getKey(), read_val_hash);
			} else {
				ChengInstrumentAPI.DoReadReturn encodedStr_hash = ChengInstrumentAPI.deHash(kvi.getTxnId(txn),
						entry.getKey() + "", curr_val);
				long read_val_hash = encodedStr_hash.val_hash;
				String real_val = encodedStr_hash.real_val;

				real_vals.put(entry.getKey(), real_val);
				real_val_hashes.put(entry.getKey(), read_val_hash);
			}
		}

		ChengInstrumentAPI.doTransactionRangeQuery(kvi.getTxnId(txn), key1, key2,
				real_val_hashes, dead_vals_hashes, req_timestamp, res_timestamp);
		return real_vals;
	}

	@Override
	public boolean rollback(Object txn) {
		boolean ret = kvi.rollback(txn);
		ChengInstrumentAPI.doTransactionRollback(kvi.getTxnId(txn));
		return ret;
	}

	@Override
	public boolean isalive(Object txn) {
		return kvi.isalive(txn);
	}

	@Override
	public long getTxnId(Object txn) {
		return kvi.getTxnId(txn);
	}

	@Override
	public boolean isInstrumented() {
		return true;
	}

	@Override
	public Object getTxn(long txnid) {
		return kvi.getTxn(txnid);
	}

	public void keyaccessed() {
	}

	public long getTraceSize() {
		return -1;
	}

	/**
	 * This is not compatible with ChengLogger which requires key_hash,
	 * but for SQL statements, range update statements may be not aware of keys.
	 */
	@Override
	public Object[] executeSQL(Object txn, String sql,
							   SimpleSQLConstants.TASK_TYPE taskType, Class cls)
			throws KvException, TxnException {
		Object[] vals = kvi.executeSQL(txn, sql, taskType, cls);
		ChengInstrumentAPI.doSQL(sql, taskType, vals);
		return vals;
	}
}
