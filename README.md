# Viper Bench

Viper bench is a component of 
the [Viper]() project.
It includes benchmarks to generate histories for 
[Viper](). 

This tutorial introduces how to build Viper bench and run it with [TiDB](https://en.pingcap.com/).

The following commands have been tested under Ubuntu 20.04.

Deploy a database cluster
---
You may deploy a TiDB cluster following the description in [production-deployment-using-tiup](https://docs.pingcap.com/tidb/dev/production-deployment-using-tiup).

Build Viper bench
---
Install jdk11

    $ sudo apt install openjdk-11-jdk

Install maven:

    $ sudo apt install maven

Compile the code:

    $ mvn install

Configure parameters
---
Modify `COBRA_FD` and `RESULT_FILE_NAME`. The former is the directory you want to store histories in and the latter the full path of the result file, which shows some simple statistics of the run. Here is an example:
```bash
RESULT_FILE_NAME: /viper/cheng/result.txt
COBRA_FD: /viper/cheng/
```

Modify the `TIDB_PASSWORD`, `TIDB_DB_URLS`, `TIDB_PORTS`, `TIDB_USERNAME`, `TIDB_DATABASE_NAME` in 
`config.yaml`. Here is an example:

```bash
TIDB_PASSWORD: "123"
TIDB_DB_URLS: ["34.185.93.160", "96.74.141.161"]
TIDB_PORTS: [4000, 4000]
TIDB_USERNAME: root
TIDB_DATABASE_NAME: mys
```

Run the benchmark
---
Now you can run a benchmark over the database `mys`. 
Make sure that it exists before running the test:
:

    $ java -ea -jar target/txnTest-1-jar-with-dependencies.jar local config.yaml

The history will be stored in the folder `~/viper` by default. You may customize it in `config.yaml` by setting 
`COBRA_FD`.

You can specify workload parameters in the config file `config.yaml`.

---

Viper bench uses a config file (for example, `config.yaml`) to specify parameters for an experiment.
Here are the important parameters and their possible values:

| parameters  | meaning and values                                                                                                                       | 
|---|:-----------------------------------------------------------------------------------------------------------------------------------------|
| `LIB_TYPE`  | which database library is used by clients (which database to connect). Only TiDB is supported here. |
| `BENCH_TYPE`  | the benchmark to run; `0` for V-BlindW, `1` for C-TPCC, `2` for V-RANGE, `3` for C-RUBiS, and `4` for C-Twitter                           |
| `DB_URL`  | the databse URL of the remote database;                               |
|`TXN_NUM`| the size of the workload (number of transactions); should be an integer                                                                  |
|`THREAD_NUM`| the number of clients; should be an integer                                                                                              |



---
If you want to generate Elle-comptaible histories to compare with `Elle`, please refer to `tidb_append/README.md`.
