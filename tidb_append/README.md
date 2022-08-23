# TiDB test based on [Jepsen](https://github.com/jepsen-io/jepsen/blob/main/doc/tutorial/index.md)

This is a component we use to generate Elle-compatible histories of TiDB. It generates random transactions, distribute them to databases and collect the responses to form histories. We use it in the paper [Viper: a fast snapshot isolation checker]() for comparason with Elle. The workload consists of read and append operations. [:r 1 [3,4]] means a read operation reads the value [3, 4] of key 1. [:append 1 2] represents that an append operation appends a value 2 to the existing value of key 1.

## Usage
### Step 1: Deploy database cluster
Before using this, you need to follow the instructions on the webpage [Deploy a TiDB Cluster Using TiUP](https://docs.pingcap.com/tidb/dev/production-deployment-using-tiup) to deploy a TiDB cluster and have the information like TiDB user, TiDB password, hostname/IP, port ready.

### Step 2: Install dependencies
You need to prepare a client node, which sends transactions to the database cluter and collects responses of them. On Ubuntu 20.04, try:

```bash
sudo apt-get install openjdk-8-jre openjdk-8-jre-headless libjna-java gnuplot graphviz

# download lein
wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
```

Place `lein` in your `$PATH` so that your shell can find it. 

### Step 3: Run the history collector
Assume that there is a database cluster of 3 nodes and their IP addresses are IP1, IP2 and IP3. Then replace the missing flags with your own values and run:

```bash
git clone git@github.com:winddd/stolon_wr.git

git checkout eurosys

cd stolon_wr

lein run test -w append --concurrency 24 --isolation snapshot-isolation --existing-tidb --node IP1 --node IP2 --node IP3 --time-limit 60 -r 500 --max-writes-per-key 10 --max-txn-length 8  --username "" --password "" --tidb-password ""  --tidb-user root --tidb-port 4000 --dbname mys 
```

The generated histories will be in `store` folder.

---
Explanations of the flags:
| parameters  | meaning and values                                                                                                                       | 
|---|:-----------------------------------------------------------------------------------------------------------------------------------------|
| `concurrency`  | the number of parallel threads                                                 |
| `isolation`    | isolation level, only snapshot isolation is supported                          |
| `time-limit`   | how long this test should run for                                              |
| `r`            | approximate request rate, in hz                                                |
| `max-txn-length`| the max length of a transaction                                                   |
|`username`     | which username to use to ssh into the database node                                                         |
|`password`| the password of the user specified by `username`                                                              |
|`dbname`|      which database to create tables in, don't change                                                      |

More about the flags, please refer to  [Jepsen doc](http://jepsen-io.github.io/jepsen/).                           

Troubleshooting
---
If `ntp`-related errors occur, try install `ntpdate` on each database node:

```bash
sudo apt install ntpdate
```


and then execute this command on the database node which reports error:
```bash
sudo -k -S -u root bash -c "cd /; ntpdate -p 1 -b time.google.com"
```