(ns jepsen.tidb.workloads.append
  "Test for transactional list append.
   this workload contains 5 mops: :i, :w, :d, :r, :range.
   "
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [pprint :refer [pprint]]
             [string :as str]]
            [dom-top.core :refer [with-retry]]
            [elle.core :as elle]
            [jepsen [checker :as checker]
             [client :as client]
             [generator :as gen]
             [util :as util :refer [parse-long]]
             [myutils :as myutils]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle.append :as append]
            [jepsen.tidb [client :as c]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sqlb]
            [slingshot.slingshot :refer [try+ throw+]]
            [elle.txn :as et]))

(def default-table-count 3)

(defn r!
  "read micro-op
  return the value or nil"
  [conn table k]
  (let [rs (j/execute! conn
                       [(str "select val from " table " where "
                             (if (< (rand) 0.5) "id" "sk")
                                          ;"id"
                             " = ? ")
                        k]
                       {:builder-fn rs/as-unqualified-lower-maps})
        rs (when-let [v (:val (first rs))]
             (mapv parse-long (str/split v #",")))]
    rs)
  )

(defn update!
  "Performs an append of a key k, adding element e. Returns true if the update
  succeeded, false otherwise."
  [conn test table k e]
  (let [res (-> conn
                (j/execute-one! [(str "update " table " set val = CONCAT(val, ',', ?)"
                                      " where id = ?") e k]))]
    (info :update res)
    (-> res
        :next.jdbc/update-count
        pos?)))

(defn insert!
  "
  true: insert successfully
  exception: unknown exception => abort txn

  try to insert for range-insert, so manual savepoint and rollback are required.
  return true/false or exception happended.
  "
  [conn test txn? table k e]
  (try
    (info :insert (j/execute! conn
                              [(str "insert into " table " (id, sk, val)"
                                    " values (?, ?, ?)")
                               k k e]))
    true
    (catch java.sql.SQLIntegrityConstraintViolationException e
      (if (re-find #"Duplicate entry" (.getMessage e))
        (do (info (if txn? "txn") "try-insert failed: " (.getMessage e))
            (when txn? (j/execute! conn ["rollback to savepoint upsert"]))
            false)
        (throw e)))))

(defn mop!
  "Executes a transactional micro-op on a connection. Returns the completed
  micro-op."
  [conn test txn? micro_op]
  (let [[f k v] micro_op
        table-count (:table-count test default-table-count)
        table (myutils/table-for table-count k)]
    (Thread/sleep (rand-int 10))
    (cond
      (= f :r)  (let [vs (r! conn table k)]
                  [f k vs])
      (= f :append)  (let [vs (str v)
                      succ (or (update! conn test table k vs)
                               (insert! conn test txn? table k vs)
                               (update! conn test table k vs)
                               (throw+ {:type     ::homebrew-upsert-failed
                                        :key      k
                                        :element  v}))]
                  [f k v])                             ; just record whether pos? instead of directly abort
      :else (throw (Exception. "ERROR: not supported mop"))
))) 

(defrecord Client [node conn initialized?]
  client/Client
  (open! [this test node]
    (let [c (c/open test node)]
      (assoc this
        :node          node
        :conn          c
        :initialized?  (atom false))))

  (setup! [_ test]
    (dotimes [i (:table-count test default-table-count)]
      ; OK, so first worrying thing: why can this throw duplicate key errors if
      ; it's executed with "if not exists"?
      (with-retry [conn  conn
                   tries 10]
                  (j/execute! conn
                              [(str "create table if not exists " (myutils/table-name i)
                                    " (id int not null primary key,
                                    sk int not null,
                                    val text)")])
                  (catch org.postgresql.util.PSQLException e
                    (condp re-find (.getMessage e)
                      #"duplicate key value violates unique constraint"
                      :dup

                      #"An I/O error occurred|connection has been closed"
                      (do (when (zero? tries)
                            (throw e))
                          (info "Retrying IO error")
                          (Thread/sleep 1000)
                          (c/close! conn)
                          (retry (c/await-open test node)
                                 (dec tries)))

                      (throw e))))
      ; Make sure we start fresh--in case we're using an existing postgres
      ; cluster and the DB automation isn't wiping the state for us.
      (j/execute! conn [(str "delete from " (myutils/table-name i))])))

  (invoke! [_ test op]
    ; One-time connection setup
    ;; (when (compare-and-set! initialized? false true)
      
    ;;   (c/set-transaction-isolation! conn (:isolation test)))

    (c/with-errors op
                   (let [txn       (:value op)
                         first-mop (get txn 0)
                         first-f   (get first-mop 0)
                         use-txn?  true
                         ;(or (< 1 (count txn)) (and (= 1 (count txn)) (.contains [:d] first-f)))
                         ;use-txn?  (< 1 (count txn))
                         txn'      (j/with-transaction
                                       [t conn
                                      ;  ]
                                        {:isolation :repeatable-read}]
                                        (mapv (partial mop! t test true) txn))]
                     (assoc op :type :ok, :value txn'))))

  (teardown! [_ test])

  (close! [this test]
    (c/close! conn)))


(defn workload
  "A list append workload."
  [opts]
  (-> (append/test (assoc (select-keys opts [:key-count
                                             :max-txn-length
                                             :max-writes-per-key])
                          :min-txn-length 1
                          :consistency-models [(:expected-consistency-model opts)]))
      (assoc :client (Client. nil nil nil))))

