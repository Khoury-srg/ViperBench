package main;

import java.io.File;
import bench.Planner;

public class Main {
	private static void printHelp() {
		System.out.println("Usage: xxx server/client/local config.yaml");
		System.exit(0);
	}

	public static void main(String[] args) {
		String configFile = null;
		if (args.length == 1) {
			System.out.println("[Warning]No config file is specified, using config.yaml by default");
			configFile = "config.yaml";
		} else if (args.length == 2) {
			configFile = args[1];
			System.out.println("using config file: " + configFile);
		} else {
			printHelp();
		}

		Config.readConfig(configFile);

		System.out.println("Your Config: \n" + Config.get().toString());

		if (args[0].equals("local")) {
			clearLogs();

			long t1 = System.currentTimeMillis();
			localMain();
			long t2 = System.currentTimeMillis();
			System.out.printf("Thie benchmark took %fs", (t2-t1) / 1000.0);
		} else {
			printHelp();
		}
		System.exit(0);
	}

	private static void clearSingleDIr(String dir){
		assert dir != null;
		File f = new File(dir);
		if(!f.exists())
			return;
		assert f.isDirectory();

		for (File log : f.listFiles()) {
			boolean done = log.delete();
			System.out.println("[INFO] delete file [" + log.toString() + "]... done?" + done);
		}
	}
	
	private static void clearLogs() {
		clearSingleDIr(Config.get().COBRA_FD);
		clearSingleDIr(Config.get().COBRA_FD_LOG);
		clearSingleDIr(Config.get().LATENCY_FOLDER);
		// clear jepsen log dir
		clearSingleDIr(Config.get().COBRA_JEPSEN_LOG);
		clearSingleDIr(Config.get().COBRA_JSON_LOG);
		System.out.println("[INFO] Clear done.");
	}

	
	private static void localMain() {
		Planner.standardProcedure();
	}
	
}