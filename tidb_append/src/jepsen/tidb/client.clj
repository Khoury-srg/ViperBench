(ns jepsen.tidb.client
    "Helper functions for interacting with PostgreSQL clients."
    (:require [clojure.tools.logging :refer [info warn]]
      [dom-top.core :refer [with-retry]]
      [jepsen [client :as client]
       [util :as util]]
      [next.jdbc :as j]
      [next.jdbc.result-set :as rs]
      [next.jdbc.sql.builder :as sqlb]
      [slingshot.slingshot :refer [try+ throw+]]
      [wall.hack :as wh])
    (:import (java.sql Connection)))

(defn open
      "Opens a connection to the given node."
      [test node]
      (let [spec  {:dbtype    (get :dbtype test "mysql")
                   :dbname    (get :dbname test "mys")
                   :host      node
                   :port      (:tidb-port     test)
                   :user      (:tidb-user     test)
                   :password  (:tidb-password test)
                   :ssl       false
                   :sslmode   "disable"
                   :connectTimeout 10000
                   :socketTimeout 10000
                   }
            spec  (if-let [pt (:prepare-threshold test)]
                          (assoc spec :prepareThreshold pt)
                          spec)
            ds    (j/get-datasource spec)
            conn  (j/get-connection ds)]
           conn))

(defn set-transaction-isolation!
  "Sets the transaction isolation level on a connection. Returns conn."
  [conn level]
  (.setTransactionIsolation
    conn
    (case level
      :repeatable-read  Connection/TRANSACTION_REPEATABLE_READ))
  conn)

(defn close!
      "Closes a connection."
      [^java.sql.Connection conn]
      (.close conn))

(defn await-open
      "Waits for a connection to node to become available, returning conn. Helpful
      for starting up."
      [test node]
      (with-retry [tries 100]
                  (info "Waiting for" node "to come online...")
                  (let [conn (open test node)]
                       (try (j/execute-one! conn
                                            ["create table if not exists jepsen_await ()"])
                            conn
                            (catch org.postgresql.util.PSQLException e
                              (condp re-find (.getMessage e)
                                     ; Ah, good, someone else already created the table
                                     #"duplicate key value violates unique constraint \"pg_type_typname_nsp_index\""
                                     conn

                                     (throw e)))))
                  (catch org.postgresql.util.PSQLException e
                    (when (zero? tries)
                          (throw e))

                    (Thread/sleep 5000)
                    (condp re-find (.getMessage e)
                           #"connection attempt failed"
                           (retry (dec tries))

                           #"Connection to .+ refused"
                           (retry (dec tries))

                           #"An I/O error occurred"
                           (retry (dec tries))

                           (throw e)))))

(defmacro with-errors
          "Takes an operation and a body; evals body, turning known errors into :fail
          or :info ops."
          [op & body]
          `(try ~@body
                (catch clojure.lang.ExceptionInfo e#
                  ; (warn e# "Caught ex-info")
                  (assoc ~op :type :fail, :error [:conn-closed-rollback-failed :ex-info (.getMessage e#)]))
                  ; (assoc ~op :type :info, :error [:ex-info (.getMessage e#)]))
                  ; (cond
                  ;   (= "createStatement() is called on closed connection" (.cause (:rollback (ex-data e#))))
                  ;   (assoc ~op :type :fail, :error :conn-closed-rollback-failed)

                  ;   (= "Rollback failed handling" (.cause (:rollback (ex-data e#))))
                  ;   (assoc ~op :type :info, :error :conn-closed-rollback-failed)


                  ;   true 
                  ;   (do 
                  ;     (info (str "error msg: " (.getMessage e#)))
                  ;     (info e# :caught (pr-str (ex-data e#)))
                  ;     (info :caught-rollback (:rollback (ex-data e#)))
                  ;     (info :caught-cause    (.cause (:rollback (ex-data e#))))
                  ;     (throw e#))
                  ;   ; (warn e# "Caught ex-info")
                  ;   ; (do (println ((.getMessage e#)))
                  ;   ; (assoc ~op :type :info, :error [:ex-info (.getMessage e#)]))
                  ;   )
                    
                    ; )

                (catch java.lang.IllegalArgumentException e#
                  (condp re-find (.getMessage e#)
                    #"Write conflict"
                    (assoc ~op :type :fail, :error [:write-conflict (.getMessage e#)])

                    #"Resolve lock timeout"
                    (assoc ~op :type :fail, :error [:resolve-lock-timeout (.getMessage e#)])
                    
                    #"TiKV server timeout"
                    (assoc ~op :type :fail, :error [:TiKV-server-timeout (.getMessage e#)])

                    #"Deadlock found"
                    (assoc ~op :type :fail, :error [:dead-lock (.getMessage e#)])

                    #"inconsistent extra index"
                    (System/exit 1)
                    ;; (assoc ~op :type :fail, :error [:dead-lock (.getMessage e#)])

                    #"No operations allowed after connection closed"
                    (assoc ~op :type :fail, :error [:connection-closed (.getMessage e#)])

                    #"Duplicate entry"
                    (assoc ~op :type :fail, :error [:duplicate-key-value (.getMessage e#)])))

                (catch java.lang.Exception e#
                  (condp re-find (.getMessage e#)
                        #"Write conflict"
                        (assoc ~op :type :fail, :error [:write-conflict (.getMessage e#)])

                         #"Resolve lock timeout"
                         (assoc ~op :type :fail, :error [:resolve-lock-timeout (.getMessage e#)])
                    
                         #"TiKV server timeout"
                         (assoc ~op :type :fail, :error [:TiKV-server-timeout (.getMessage e#)])

                         #"ERROR: insert failed, duplicate key"
                         (assoc ~op :type :fail, :error [:duplicate-key-value (.getMessage e#)])

                         ;#"No matching clause: Key"
                         ;(assoc ~op :type :fail, :error [:duplicate-key-value (.getMessage e#)])
                         #"Lock wait timeout exceeded"
                         (assoc ~op :type :fail, :error [:lock-wait-time-out (.getMessage e#)])

                         #"Deadlock found"
                         (assoc ~op :type :fail, :error [:dead-lock (.getMessage e#)])

                         #"Duplicate entry"
                         (assoc ~op :type :fail, :error [:duplicate-key-value (.getMessage e#)])

                         #"ERROR: delete failed, key not exists"
                         (assoc ~op :type :fail, :error [:key-not-exists (.getMessage e#)])

                         #"No operations allowed after connection closed"
                         (assoc ~op :type :fail, :error [:connection-closed (.getMessage e#)])
                        
                         #"ERROR: inconsistent view"
                         (assoc ~op :type :fail, :error [:inconsistent-view (.getMessage e#)])


                         ))
                ))


