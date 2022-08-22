#!/bin/bash
CONCURRENCY=24 # run1: 12
# WORKLOAD=ra
WORKLOAD=append
# TIME_LIMIT=10
R=500
MAX_WRITES_PER_KEY=10
MAX_TXN_LENGTH=16
PORT=4000
# NEMESIS=partition
# TESTTIMES=2

# N1=instance-5
# N1=34.148.183.230
N2=34.148.93.160
N3=34.74.141.161
N4=34.74.141.161

# --node $N4 --node $N2
# for i in {1..20}
for i in {1..1}
  do
    # for j in $N2 $N3 $N4
    # do
    #   ssh "$j" "sudo service ntp stop"
    #   ssh "$j" "sudo ntpdate pool.ntp.org"
    #   ssh "$j" "sudo -k -S -u root bash -c "cd /; ntpdate -p 1 -b time.google.com""
    # done

    # TIME_LIMIT=`expr $i \* 10`
    TIME_LIMIT=30
    lein run test -w $WORKLOAD --concurrency $CONCURRENCY --isolation snapshot-isolation --existing-tidb  \
    --node $N2 --node $N3 --node $N4 --time-limit  $TIME_LIMIT -r $R --max-writes-per-key $MAX_WRITES_PER_KEY --max-txn-length $MAX_TXN_LENGTH  \
    --username windkl --password head..java --tidb-password "viper321"  --tidb-user root --tidb-port $PORT --dbname mys 

 done