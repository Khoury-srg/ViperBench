package bench.twitter;

import bench.BenchUtils;
import bench.Benchmark;
import bench.Transaction;
import bench.ZipfianGenerator;
import kv_interfaces.KvInterface;
import main.Config;

public class TwitterBench extends Benchmark {
	private int lastTweetId = Config.get().CLIENT_ID*10000000;
	private final int USER_NUM;
	private final ZipfianGenerator zipf;
	private final ZipfianGenerator activateUserZipf;

	public TwitterBench(KvInterface kvi) {
		super(kvi);
		USER_NUM = Config.get().TWITTER_USERS_NUM;
		zipf = new ZipfianGenerator(USER_NUM);
		activateUserZipf = new ZipfianGenerator(USER_NUM, 0.5);
	}

	@Override
	public Transaction[] preBenchmark() {
		Transaction[] ret = new Transaction[USER_NUM];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = new LoadTwitter(kvi, i, true);
		}
		this.lastTweetId += USER_NUM * TwitterConstants.TWEETS_PER_USER;
		return ret;
	}

	@Override
	public Transaction getNextTxn() {
		Transaction t = null;
		int userId = BenchUtils.getRandomInt(0, USER_NUM);
//		int userId = activateUserZipf.nextValue().intValue(); // how to make sure this key already exists in the db?? getLastTweet null?

		int dice = BenchUtils.getRandomInt(0, 100);
		if (dice < 20) {
			t = new TxnNewTweet(kvi, userId, ++lastTweetId, false);
		} else if (dice < 60) {
			int dst = BenchUtils.getRandomInt(0, USER_NUM);
//			int dst = zipf.nextValue().intValue();
			t = new TxnFollow(kvi, userId, dst, false);
		} else if (dice < 70) {
			t = new TxnTimeline(kvi, userId, false);
		} else if (dice < 80) {
			t = new TxnShowFollow(kvi, userId, false);
		} else {
			t = new TxnShowTweets(kvi, userId, false);
		}

		return t;
	}

	@Override
	public Transaction getFinalTransaction() {
		return null;
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
		return new String[] { "Follow", "NewTweet", "ShowFollow", "ShowTweets", "Timeline", "kvi" };
	}

}
