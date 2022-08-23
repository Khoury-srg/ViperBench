#!/bin/bash
CONCURRENCY=24
WORKLOAD=append
R=500
MAX_WRITES_PER_KEY=10
MAX_TXN_LENGTH=16
PORT=4000

N2=db-instance-2
N3=db-instance-3
N4=db-instance-4

for i in {1..20}
 do
    # for j in {2..4}
    # do
    #   ssh "db-instance-$j" "sudo service ntp stop"
    #   ssh "db-instance-$j" "sudo ntpdate pool.ntp.org"
    # done

    TIME_LIMIT=`expr $i \* 10`
    lein run test -w $WORKLOAD --concurrency $CONCURRENCY --isolation snapshot-isolation --existing-tidb  \
    --node $N2 --node $N3 --node $N4 --time-limit  $TIME_LIMIT -r $R --max-writes-per-key $MAX_WRITES_PER_KEY --max-txn-length $MAX_TXN_LENGTH  \
    --username "" --password "" --tidb-password ""  --tidb-user root --tidb-port $PORT --dbname mys 

 done