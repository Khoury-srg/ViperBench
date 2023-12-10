package kv_interfaces;

import bench.simpleSQL.SimpleSQLConstants;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

import java.util.ArrayList;
import java.util.Map;

public interface KvInterface {
	
	public Object begin() throws KvException,TxnException;
	public boolean commit(Object txn, boolean isInitial, boolean isFinal) throws KvException,TxnException;
	public boolean abort(Object txn) throws KvException,TxnException;
	// KV store API
	public boolean insert(Object txn, String key, String value) throws KvException,TxnException;
	public boolean delete(Object txn, String key) throws KvException,TxnException;
	public String get(Object txn, String key) throws KvException,TxnException;
	public boolean set(Object txn, String key, String value) throws KvException,TxnException;
	public Map<Long, String> range(Object txn, String k1, String key2) throws KvException,
			TxnException;

	// SQL API
	public Object[] executeSQL(Object txn, String sql,
							 SimpleSQLConstants.TASK_TYPE taskType, Class cls) throws KvException, TxnException;

	// the client is responsible to call rollback() when it catches a TxnExecption
	// and if the client wants to manually abort a txn, then only abort() is needed to call.
	public boolean rollback(Object txn);
	public boolean isalive(Object txn);
	public long getTxnId(Object txn);
	public Object getTxn(long txnid);
	public boolean isInstrumented();
	
}