package bench.rubis;

import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public class FinalTransaction extends RubisTransaction{
    public FinalTransaction(KvInterface kvi, String name, boolean preBench) {
        super(kvi, name, preBench);
    }

    @Override
    public void inputGeneration() {

    }

    @Override
    public boolean doTansaction() throws KvException, TxnException {
        beginTxn();
        kvi.range(txn, null, null);
        commitTxn(isInitialTxn, isFinalTxn);
        return true;
    }
}
