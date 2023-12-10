package bench.simpleSQL;

import bench.Transaction;
import bench.chengTxn.ChengTxnConstants;
import bench.simpleSQL.SimpleSQLConstants.TASK_TYPE;
import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;
import org.apache.commons.lang3.tuple.Pair;

public class SimpleSQLTxn extends Transaction {
    private String sqlTempalte;
    private TASK_TYPE taskType;

    public int[] getRegularArgs() {
        return regularArgs;
    }

    private int[] regularArgs;
    private int[] writtenValues;
    private String sql;
    private int numRegularArgs;
    private int numWrittenValues;

    public SimpleSQLTxn(int[] regularArgs){
        super(null, null, false);
        this.regularArgs = regularArgs;
    }

    public SimpleSQLTxn(KvInterface kvi, String sqlTemplate, TASK_TYPE taskType, int[] regularArgs, int[] writtenValues,
                        boolean preBench){
        super(kvi, getOpTag(taskType), preBench);
        this.sqlTempalte = sqlTemplate;
        this.taskType = taskType;
        this.regularArgs = regularArgs;
        this.writtenValues = writtenValues;
        this.txnName = ChengTxnConstants.TaskType2TxnTag.get(taskType);

        Pair<Integer, Integer> numArgsPair = SimpleSQLConstants.numArgs.get(taskType);
        numRegularArgs = numArgsPair.getLeft();
        numWrittenValues = numArgsPair.getRight();

        sql = sqlTemplate;
        if(taskType != TASK_TYPE.INSERT){
            for(int i = 0; i < numRegularArgs; i++)
                sql = sql.replaceFirst("\\?", regularArgs[i] + "");
            for(int i = 0; i < numWrittenValues; i ++)
                sql = sql.replaceFirst("%d", writtenValues[i] + "");
            sql = sql.replace("%s", SimpleSQLConstants.table);
        }
    }

    private static String getOpTag(TASK_TYPE taskType) {
		String tag = SimpleSQLConstants.TaskType2TxnTag.get(taskType);
		return tag;
	}

    @Override
    public void inputGeneration() {    }

    @Override
    public boolean doTansaction() throws KvException, TxnException {
        beginTxn();
        if(taskType != TASK_TYPE.INSERT)
            kvi.executeSQL(txn, sql, taskType, null);
        else { // we allow multiple operations for the insertion txn
            for(int i = 0; i < regularArgs.length; i++){ // i-th operation
                sql = sqlTempalte;

                for(int j = 0; j < numRegularArgs; j++)
                    sql = sql.replaceFirst("\\?", regularArgs[i+j] + "");
                for(int j = 0; j < numWrittenValues; j ++)
                    sql = sql.replaceFirst("%d", writtenValues[2*i+j] + "");
                sql = sql.replace("%s", SimpleSQLConstants.table);
                kvi.executeSQL(txn, sql, taskType, null);
            }
        }

        commitTxn(isInitialTxn, isFinalTxn);
        return true;
    }
}
