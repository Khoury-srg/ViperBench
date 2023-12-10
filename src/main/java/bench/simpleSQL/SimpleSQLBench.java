package bench.simpleSQL;

import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import bench.BenchUtils;
import bench.Benchmark;
import bench.Transaction;
import bench.chengTxn.ChengTransaction;
import bench.chengTxn.ChengTxnConstants;
import bench.simpleSQL.SimpleSQLConstants.TASK_TYPE;
import kv_interfaces.KvInterface;
import main.Config;
import org.apache.commons.lang3.tuple.Pair;

public class SimpleSQLBench extends Benchmark {
    private static AtomicInteger keyNum;
    private Random rand = new Random(Config.get().SEED);
    private static AtomicInteger writtenValue;

    public SimpleSQLBench(KvInterface kvi) {
        super(kvi);
        keyNum = new AtomicInteger(Config.get().KEY_INDX_START);
        if (Config.get().SKIP_LOADING) {
            keyNum.addAndGet(Config.get().NUM_KEYS);
        }
        writtenValue = new AtomicInteger(1);
    }

    private int getNewKey() {
        int cur_indx = keyNum.getAndIncrement();
        return cur_indx;
    }

    private int getExistingKey() {
        int cur_indx = keyNum.get();
        if (Config.get().KEY_INDX_START == cur_indx)
            return Config.get().KEY_INDX_START;
        return BenchUtils.getRandomInt(Config.get().KEY_INDX_START, cur_indx);
    }

    @Override
    public Transaction[] preBenchmark() {
        return new Transaction[0];
    }

    private TASK_TYPE nextOpType() {
        int dice = rand.nextInt(Config.get().RATIO_INSERT +
                Config.get().RATIO_READ +
                Config.get().RATIO_UPDATE +
                Config.get().RATIO_DELETE);
        int[] ratios = new int[] { Config.get().RATIO_INSERT,
                Config.get().RATIO_READ,
                Config.get().RATIO_UPDATE,
                Config.get().RATIO_DELETE }; // hardcode
        int[] sumDice = new int[4];
        int sum = 0;
        for (int i = 0; i < sumDice.length; i++) {
            sum += ratios[i];
            sumDice[i] = sum;
        }

        int i = 0;
        while (dice >= sumDice[i]) {
            i++;
        }
        assert (i < sumDice.length);
        TASK_TYPE[] taskTypes = new TASK_TYPE[] { TASK_TYPE.INSERT,
                TASK_TYPE.SELECT, TASK_TYPE.UPDATE, TASK_TYPE.DELETE };
        return taskTypes[i];
    }

    @Override
    public Transaction getNextTxn() {
        TASK_TYPE op = nextOpType();
        return getTheTxn(op, false);
    }

    public SimpleSQLTxn getTheTxn(TASK_TYPE taskType, boolean preBench) {
        String template = SimpleSQLConstants.sqlTemplates.get(taskType);
        int nRegularArgs = numRegularArgs(template);
        int nWrittenValues = numWrittenValues(template);

        int regularLength = taskType != TASK_TYPE.INSERT? nRegularArgs: Config.get().OP_PER_CHENGTXN;
        int writtenLength = taskType != TASK_TYPE.INSERT? nWrittenValues: Config.get().OP_PER_CHENGTXN * nWrittenValues;
        int[] regularArgs = new int[regularLength];
        int[] writtenValues = new int[writtenLength];

        int MAX_INT = 1000;

        switch (taskType) {
            case INSERT:
                for(int i = 0; i < Config.get().OP_PER_CHENGTXN; i++){
                    regularArgs[i] = getNewKey();
                    writtenValues[2*i] = writtenValue.getAndIncrement();
                    writtenValues[2*i+1] = writtenValue.getAndIncrement();
                }

                break;
            case UPDATE:
                regularArgs[0] = rand.nextInt(MAX_INT);
                writtenValues[0] = writtenValue.getAndIncrement();
                break;
            case DELETE:
                regularArgs[0] = getExistingKey();
                break;
            case SELECT:
                regularArgs[0] = rand.nextInt(writtenValue.get());
                regularArgs[1] = getExistingKey();
                break;
            default:
                assert false;
        }

        SimpleSQLTxn nextTxn = new SimpleSQLTxn(kvi, template, taskType, regularArgs, writtenValues, preBench);
        return nextTxn;
    }

    private int numRegularArgs(String template) {
        // assume that the %d must be separated from other characters with a space in
        // the template
        int numRegArgs = 0;
        for (int i = 0; i < template.length(); i++)
            if (template.charAt(i) == '?')
                numRegArgs++;

        return numRegArgs;
    }

    private int numWrittenValues(String template) {
        int numWrittenValues = 0;
        String[] words = template.split(" ");
        for (String word : words)
            if (word.equals("%d"))
                numWrittenValues++;

        return numWrittenValues;
    }

    @Override
    public Transaction getFinalTransaction() {
//        System.out.println(String.format("keyNum=%d", this.keyNum.get()));
        return new SimpleSQLTxn(new int[]{this.keyNum.get()});
    }

    @Override
    public Transaction getInitialTransaction() {
        return null;
    }

    @Override
    public void afterBenchmark() {
    }

    @Override
    public String[] getTags() {
        return new String[] {
                SimpleSQLConstants.TXN_INSERT_TAG,
                SimpleSQLConstants.TXN_DELETE_TAG,
                SimpleSQLConstants.TXN_SELECT_TAG,
                SimpleSQLConstants.TXN_UPDATE_TAG,
                "kvi" };
    }

}
