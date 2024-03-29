package bench.rubis;

import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public class NewRegion extends RubisTransaction {
	private int regionId;
	private String regionName;

	public NewRegion(KvInterface kvi, int regionId, boolean preBench) {
		super(kvi, "NewRegion", preBench);
		this.regionId = regionId;
	}

	@Override
	public void inputGeneration() {
		regionName = "region" + regionId;
	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		beginTxn();
		boolean res = insertRegion(regionName, regionId);
		commitTxn(isInitialTxn, isFinalTxn);
		return res;
	}

}
