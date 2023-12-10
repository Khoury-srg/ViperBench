package bench;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;

import kv_interfaces.*;
import main.Config;

public abstract class Benchmark {
	protected KvInterface kvi;
	private static String googleKind = null;

	public static KvInterface getKvi(Config.LibType libtype, boolean useInstrument) {
		KvInterface kvi = null;
		if (libtype == Config.LibType.ROCKSDB_ORIG_LIB) {
			kvi = RocksDBKV.getInstance();
		} else if (libtype == Config.LibType.CHENG_ORIG_LIB) {
			assert false;
		} else if (libtype == Config.LibType.GOOGLE_DATASTORE_LIB) {
			// use a random kind to make sure we launch a new kind every time
			// because google limits the operation rate and makes it slow start
			if (googleKind == null) {
				synchronized (Benchmark.class) {
					if (googleKind == null) {
						googleKind = Config.getTableName(Config.get().BENCH_TYPE) + "-" + BenchUtils.getRandomValue(8);
					}
				}
			}
			Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
			kvi = new GoogleDataStore(datastore, googleKind);
		} else if (libtype == Config.LibType.RPC_CLIENT_LIB) {
			assert false;
			//kvi = RpcClient.getInstance();
		} else if (libtype == Config.LibType.POSTGRESQL_LIB) {
//			String tableName = Config.getTableName(Config.get().BENCH_TYPE);
//			kvi = new SqlKV(tableName);
			kvi = PostgresKV.getInstance();
		} else if (libtype == Config.LibType.COCKROACH_LIB) {
			kvi =  CockroachDB.getInstance();
		} else if (libtype == Config.LibType.YUGABYTE_LIB) {
			kvi =  YugaByteDB.getInstance();
		} else if (libtype == Config.LibType.TIDB) {
			kvi = TiDB.getInstance();
		} else if (libtype == Config.LibType.SQLSERVER) {
			kvi = SQLServer.getInstance();
		} else if (libtype == Config.LibType.H2) {
			kvi = H2.getInstance();
		} else if (libtype == Config.LibType.POSTGRESQL_DB) {
			kvi = PostgreSQLDB.getInstance("simplesql");
		} else {
			// should not be here
			assert false;
		}

		if(useInstrument) {
			return new InstKV(kvi);
		} else {
			return kvi;
		}
	}

	public Benchmark(KvInterface kvi) {
		this.kvi = kvi;
	}

	// things to do before running benchmark: initialize the database / generate something / ...
	// TODO: is returning type Transaction suitable?
	public abstract Transaction[] preBenchmark();

	// feed the planner
	public abstract Transaction getNextTxn();
	public abstract Transaction getFinalTransaction();
	public abstract Transaction getInitialTransaction();


	// clean-up after benchmark
	public abstract void afterBenchmark();

	public abstract String[] getTags();
}
