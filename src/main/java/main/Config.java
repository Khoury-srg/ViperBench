package main;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.*;

import kvstore.exceptions.WrongConfigException;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import static main.Config.BenchType.*;

public class Config {
	// Thread unsafe singleton
	private static Config instance = null;

	public static Config get() {
		assert instance != null;
		return instance;
	}

	public static void readConfig(String configFile) {
		if (instance == null) {
			ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
			try {
				instance = mapper.readValue(new File(configFile), Config.class);
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		} else {
			System.err.println("Error: the config has already been loaded");
			System.exit(-1);
		}

		Set<BenchType> integerBenches = new HashSet<>();
		integerBenches.add(YCSB);
		integerBenches.add(CHENG);
		integerBenches.add(SIMPLE_SQL);

		Set<BenchType> stringBenches = new HashSet<>();
		stringBenches.add(RUBIS);
		stringBenches.add(TWITTER);
		stringBenches.add(TPCC);

		if (instance.KEY_TYPE == 0 && !stringBenches.contains(instance.BENCH_TYPE)) {
			throw new WrongConfigException();
		}

		if (instance.KEY_TYPE == 1 && !integerBenches.contains(instance.BENCH_TYPE)) {
			throw new WrongConfigException();
		}

		String CONFIGS = String.format("%s-%dtxns-%doppertxn-threads%d-keys%d-I%d-D%d-R%d-U%d-RANGEE%d-SI%d",
				instance.benchTagMap.get(instance.BENCH_TYPE),
				instance.TXN_NUM,
				instance.OP_PER_CHENGTXN,
				instance.THREAD_NUM,
				instance.NUM_KEYS,
				instance.RATIO_INSERT,
				instance.RATIO_DELETE,
				instance.RATIO_READ,
				instance.RATIO_UPDATE,
				instance.RATIO_RANGE,
				instance.ISOLATION_LEVEL);
		if (instance.BENCH_TYPE == RUBIS) { // rubis
			CONFIGS += "-users" + instance.RUBIS_USERS_NUM;
		} else if (instance.BENCH_TYPE == TWITTER) {
			CONFIGS += "-users" + instance.TWITTER_USERS_NUM;
		}

		String timeStamp = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date());

		instance.COBRA_FD = String.format("%s%s-%s-%s/", instance.COBRA_FD, instance.TAG, CONFIGS, timeStamp);
		instance.COBRA_FD_LOG = String.format("%slog/", instance.COBRA_FD);
		instance.COBRA_JEPSEN_LOG = String.format("%sjepsen/", instance.COBRA_FD);
		instance.COBRA_JSON_LOG = String.format("%sjson/", instance.COBRA_FD);
		instance.COBRA_SQL_LOG = String.format("%ssql/", instance.COBRA_FD);
		instance.COBRA_SQL_COMBINED_LOG = String.format("%scombined_sql.log", instance.COBRA_FD);
		instance.BENCHMARK_LOG_PATH = String.format("%sbenchmark.log", instance.COBRA_FD);
		instance.RESULT_FILE_NAME = String.format("%sresult.txt", instance.COBRA_FD);
		instance.LATENCY_FOLDER = String.format("%slatency/", instance.COBRA_FD);
	}

	public static String getTableName(BenchType b) {
		switch (b) {
			case CHENG:
				return "chengTxn";
			case TPCC:
				return "tpcc";
			case YCSB:
				return "ycsb";
			case RUBIS:
				return "rubis";
			case TWITTER:
				return "twitter";
			default:
				assert false;
				break;
		}
		return "";
	}

	public int getIsolationLevel() {
		switch (ISOLATION_LEVEL) {
			case 1:
				return Connection.TRANSACTION_SERIALIZABLE;
			case 2:
			case 3:
				// YugaByte: this is Snapshot Isolation
				// see:
				// https://docs.yugabyte.com/v1.2/architecture/transactions/isolation-levels/
				return Connection.TRANSACTION_REPEATABLE_READ;
			case 4:
				return Connection.TRANSACTION_READ_COMMITTED;
			case 5:
				return Connection.TRANSACTION_READ_UNCOMMITTED;
			default:
				assert false;
				return 0;
		}
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}

	/**
	 * POSTGRESQL_DB is different from POSTGRESQL_LIB, the former one is sql db, the
	 * latter is KV for compatibility.
	 */
	public enum LibType {
		CHENG_ORIG_LIB, GOOGLE_DATASTORE_LIB, ROCKSDB_ORIG_LIB, POSTGRESQL_LIB, RPC_CLIENT_LIB,
		TAPIR_LIB, COCKROACH_LIB, YUGABYTE_LIB, TIDB, SQLSERVER, H2, POSTGRESQL_DB
	};

	public enum CCType {
		WAIT_YOUNG_DIE, WAIT_OLD_DIE,
	};

	public enum BenchType {
		CHENG, TPCC, YCSB, RUBIS, TWITTER,
		WRITESKEW, ROANOMALY, WOROANOMALY,
		SIMPLE_SQL
	};

	public Map<BenchType, String> benchTagMap = Map.of(
			CHENG, "Cheng",
			TPCC, "TPCC",
			YCSB, "YCSBT",
			RUBIS, "Rubis",
			TWITTER, "Twitter",
			WRITESKEW, "WriteSkew",
			ROANOMALY, "ROAnomaly",
			WOROANOMALY, "WOROAnomaly",
			SIMPLE_SQL, "Simple_SQL");

	// mode
	public LibType LIB_TYPE = LibType.CHENG_ORIG_LIB;
	public CCType CC_TYPE = CCType.WAIT_YOUNG_DIE;
	public BenchType BENCH_TYPE = BenchType.TPCC;
	public boolean TIMESTAMP_ON = false;

	// log related
	// RESULT_FILE_NAME: /tmp/cobra/cheng/result.txt
	// COBRA_FD: /tmp/cobra/cheng/
	// COBRA_FD_LOG: /tmp/cobra/cheng/log/
	// COBRA_JEPSEN_LOG: /tmp/cobra/cheng/jepsen/
	// LATENCY_FOLDER: /tmp/cobra/cheng/latency/
	public String COBRA_FD = "/tmp/cobra/";
	public String COBRA_FD_LOG = "/tmp/cobra/log/";
	public String COBRA_JEPSEN_LOG = "/tmp/cobra/log/jepsen";
	public String COBRA_JSON_LOG = "/tmp/cobra/log/json";
	public String COBRA_SQL_LOG = "/tmp/cobra/log/sql";
	public String COBRA_SQL_COMBINED_LOG = "/tmp/cobra/log/";
	public String BENCHMARK_LOG_PATH = "/tmp/cobra/benchmark.log";
	public boolean APPEND_OTHERWISE_RECREATE = false;

	public boolean DEBUG_LIB_FLAG = false; // this is a flag within ChengInstrumentAPI
	public boolean LOG_DEBUG = false;
	public boolean LOG_INFO = false;
	public boolean LOG_ERROR = true;

	// config
	public long SEED = 20181120;

	// -------------Instrumented lib related--------------------
	public boolean USE_INSTRUMENT = true;

	public boolean LOCAL_LOG = true;
	public boolean LOCAL_REMOTE_LOG = false; // local log, but send to the verifier through socket
	public boolean CLOUD_LOG = false;
	public boolean SIGN_DATA = false;

	public int MAX_FZ_TXN_NUM = 100;
	public int NUM_TXN_IN_ENTITY = 1;

	public String KEY_CLIENT_LOG_SUFFIX = "_CL";
	public String EPOCH_KEY = "FZVERSION";
	public final String EPOCH_CLIENTS_SEP_STR = ";"; // for value within EPOCH_KEY
	public final String EPOCH_CLIENT_EPOCH_SEP_STR = ":";

	public boolean USE_NEW_EPOCH_TXN = false;

	// ----------------- Benchmarking ----------------------
	public boolean SKIP_LOADING = false;
	public boolean SKIP_TESTING = false;
	public boolean ENABLE_BARRIER = false;
	public boolean DONT_RECORD_LOADING = true;
	public int TXN_NUM = 1000;
	public int CLIENT_ID = 1;
	public int THREAD_NUM = 8;
	public int CLIENT_NUM = 1;
	// benchmark planner sleeptime(ms)
	public int SLEEP_TIME = 1000;
	// dump result to:
	public String RESULT_FILE_NAME = "result.txt";
	public String LATENCY_FOLDER = "/tmp/cobra/latency/";
	public String REDIS_ADDRESS = "redis://localhost/0";

	/**
	 * Workloads
	 */

	// --------------chengTxn/ycsb related---------------
	public int OP_PER_CHENGTXN = 8;
	public int VALUE_LENGTH = 140;
	public String KEY_PRFIX = "";
	public int KEY_INDX_START = 1;

	public int NUM_KEYS = 1000;
	public int RATIO_INSERT = 0;
	public int RATIO_READ = 50;
	public int RATIO_UPDATE = 50;
	public int RATIO_DELETE = 0;
	public int RATIO_RMW = 0;
	public int RATIO_RANGE = 0;
	public int RATIO_RANGE_VALUE = 0;

	// ---------------TPC-C related---------------
	public int WAREHOUSE_NUM = 1;
	public boolean REPORT_NEWORDER_ONLY = false;

	// ---------------RUBIS related--------------
	public int RUBIS_USERS_NUM = 1000;

	// -------------TWITTER related--------------
	public int TWITTER_USERS_NUM = 1000;

	/*************
	 * Sockets *
	 *************/

	public int VERIFIER_PORT = 10086;
	public String VERIFIER_HOSTNAME = "localhost";
	public int THROUGHPUT_PER_WAIT = 1000; // throughput = 1000/.5 = 2k
	public int WAIT_BETWEEN_TXNS = 500; // ms
	// gc
	public String GC_KEY = "COBRA_GC_KEY";

	/*************
	 * Databases *
	 *************/

	// -----------RPC related----------------
	public int SERVER_PORT = 8980;
	public String SERVER_HOST = "localhost";

	// PostgreSQL related
	public String PG_PASSWORD = "";
	public String PG_USERNAME = "postgres";
	public String PG_DATABASE_NAME = "postgres";
	public String DB_URL = "";
	public String[] PG_DB_HOSTS = { "127.0.0.1" };
	public int[] PG_PORTS = { 5432 };

	// ------------google cloud store related------------
	public String GOOGLEVALUE = "value";

	// ---------------rocksdb related-----------------
	public String ROCKSDB_PATH = "/tmp/rocksdb";

	// 1: SERIALIZABLE, 2: SNAPSHOT ISOLATION, 3: REPEATABLE READ, 4: READ
	// COMMITTED, 5: READ UNCOMMITTED
	public int ISOLATION_LEVEL = 1;

	// ------------ Cockroach DB related------------
	public String COCKROACH_PASSWORD = "";
	public String COCKROACH_USERNAME = "";
	public final String yak = "172.24.71.222";
	public final String boa = "172.24.71.208";
	public final String ye = "216.165.70.10";
	public String[] COCKROACH_DB_URLS = { yak, boa, ye };
	public int[] COCKROACH_PORTS = { 26257, 26257, 26257 };
	public String COCKROACH_DATABASE_NAME = "cobra";

	// ---------- YugaByteDB related ------------
	public String YUGABYTE_PASSWORD = "";
	public String YUGABYTE_USERNAME = "";
	public String[] YUGABYTE_DB_URLS = { "127.0.0.1", "127.0.0.2", "127.0.0.3" };
	public int[] YUGABYTE_PORTS = { 5433, 5433, 5433 };
	public String YUGABYTE_DATABASE_NAME = "cobra";

	// ---------- TiDB related ------------
	public String TIDB_PASSWORD = "";
	public String TIDB_USERNAME = "";
	public String[] TIDB_DB_URLS = { "127.0.0.1", "127.0.0.2", "127.0.0.3" };
	public int[] TIDB_PORTS = { 4000, 4000, 4000 };
	public String TIDB_DATABASE_NAME = "cobra";

	// H2 related
	public String H2_PASSWORD = "";
	public String[] H2_DB_URLS = {};
	public int[] H2_PORTS = { 9092 };
	public String H2_USERNAME = "sa";
	public String H2_DATABASE_NAME = "test";

	// ---------- SQLServer related ------------
	public String SQLSERVER_PASSWORD = "1314..zJ";
	public String[] SQLSERVER_DB_URLS = { "35.203.87.103" };
	public int[] SQLSERVER_PORTS = { 1433 };
	public String SQLSERVER_USERNAME = "SA";
	public String SQLSERVER_DATABASE_NAME = "mys";

	public int NUM_REPLICA = 3;
	public int NUM_KEY_PERTABLE = 2;
	public int ROANOMALY_NUM_RO = 2;
	public int WOROANOMALY_NUM_WORO = 2;
	public int CRACK_ACTIVE_ABORT_RATE = 0;

	public int KEY_TYPE = 0;
	public String TAG = "NotInitialized";
	// 1: original json format, used in Viper; 2: latest json format, used in
	// Boomslang
	public int JSON = 1;
	public boolean PRINT_TS = false;
}