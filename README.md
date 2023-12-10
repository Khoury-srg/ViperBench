# Viper Bench

*Viper: A Fast Snapshot Isolation Checker* is a research paper studying checking snapshot isolation of black-box databases. Viper bench is a component of the Viper project.
It includes benchmarks to generate histories for Viper.


This tutorial mainly introduces how to build `Viper` bench and run it with [TiDB](https://en.pingcap.com/).

## Project organization
- src/main/java: TiDB's Viper history collector.
  - bench: the benchmarks used in the paper.
  - kv_interface: functions for interacting with databases.
  - kvstore/exceptions: user-defined exceptions.
  - main: the main function, default configurations, logger and profiler.
- tidb_append/src/jepsen: TiDB's Jepsen history collector.
  - drivers: the main program.
  - tidb: database-specific modules.
    - db.clj: setup-related information of the database cluster.
    - client.clj: functions for interacting with databases.
    - workloads/append.clj: defines the append workload and the operations of the workload.


The following commands have been tested under Ubuntu 20.04.

## Deploy a database cluster

You may deploy a TiDB cluster following the description in [production-deployment-using-tiup](https://docs.pingcap.com/tidb/dev/production-deployment-using-tiup).

## Build Viper bench

Install jdk18

    $ sudo apt install openjdk-18-jdk

Install maven:

    $ sudo apt install maven

Compile the code:

    $ mvn install

## Configure benchmark parameters

You may use the config files we provide, e.g. `config_BlindW_RW.yaml`, or customize the parameters in the `config.yaml` by yourself.

### Use existing config files
We provided the following config files: `config_BlindW_RM.yaml`, `config_BlindW-RW.yaml`, `config_Range_IDH.yaml`, `config_Range_RQH.yaml`, `config_rubis.yaml`, `config_tpcc.yaml`, `config_twitter.yaml`, `config_V-Range-B.yaml`. Each of them corresponds to a benchmark in the Figure 10 of the paper. To generate histories for Figure 8, please use `config_BlindW_RM.yaml` and change the value of `TXN_NUM`.

### Customize parameters
Modify `COBRA_FD` and `RESULT_FILE_NAME`. The former is the directory you want to store histories in and the latter the full path of the result file, which shows some simple statistics of the run. Here is an example:
```bash
RESULT_FILE_NAME: /viper/result.txt
COBRA_FD: /viper/
```

See [Viper bench configuration](#config) for how to update `config.yaml` and specify workload parameters.

## Configure database parameters
Here, we take `TiDB` as an example. You may also choose to modify PostgreSQL configuration parameters in `config.yaml`.
Modify the `TIDB_PASSWORD`, `TIDB_DB_URLS`, `TIDB_PORTS`, `TIDB_USERNAME`, `TIDB_DATABASE_NAME` in
`config.yaml`. Here is an example below. Note that the database name is hardcoded.

```bash
TIDB_PASSWORD: "123"
TIDB_DB_URLS: ["34.185.93.160", "96.74.141.161"]
TIDB_PORTS: [4000, 4000]
TIDB_USERNAME: root
TIDB_DATABASE_NAME: mys
```

## Run the benchmark
Now you can run a benchmark over the database `mys`.
Make sure that it exists before running the test:

    $ java -ea -jar target/txnTest-1-jar-with-dependencies.jar local config.yaml

The history will be stored in the folder `/tmp/viper` by default. You may customize it in `config.yaml` by setting
`COBRA_FD`.

<a name='config' /> Viper bench configuration
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