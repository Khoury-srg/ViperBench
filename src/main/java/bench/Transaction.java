package bench;

import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import main.Logger;

public abstract class Transaction {
	protected Object txn;
	protected KvInterface kvi;
	protected String txnName = "undefined";
	protected boolean preBench = false;
	protected boolean isInitialTxn = false;
	protected boolean isFinalTxn = false;
	
	public abstract void inputGeneration();
	
	// returning true means the txn is committed successfully, false means the txn is aborted.
	public abstract boolean doTansaction() throws KvException, TxnException;

	protected void check_txn_exists() {
		assert txn != null && kvi != null;
	}

	public Transaction(KvInterface kvi, String name, boolean preBench) {
		txn = null;
		this.kvi = kvi;
		this.txnName = name;
		this.preBench = preBench;
	}

	public void setNewKvi(KvInterface kvi) {
		this.kvi = kvi;
	}
	
	protected void beginTxn() throws KvException, TxnException {
		if(txn != null) {
			System.out.println("txn not null: " + txn);
		}
		if(kvi == null) {
			System.out.println("kvi is null");
		}
		assert txn == null && kvi != null;
		txn = kvi.begin();
		Logger.logDebug("transaction " + txn + " begins");
	}
	
	protected boolean commitTxn(boolean isInitial, boolean isFinal) throws KvException, TxnException {
		check_txn_exists();
		boolean res = kvi.commit(txn, isInitial, isFinal);
		Logger.logDebug("commitTxn: " + txn);
		txn = null;
		return res;
	}
	
	protected boolean abortTxn() throws KvException, TxnException {
		check_txn_exists();
		return kvi.abort(txn);
	}
	
	public void rollback() {
		check_txn_exists();
		kvi.rollback(txn);
	}
	
	public String getname() {
		return txnName;
	}
	
	public static class EndSignal extends Transaction{
		public EndSignal(boolean preBench) {
			super(null, "END-SIGNAL", preBench);
		}

		@Override
		public void inputGeneration() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public boolean doTansaction() throws KvException, TxnException {
			// TODO Auto-generated method stub
			return false;
		}


	}


}
