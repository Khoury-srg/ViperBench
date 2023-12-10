(ns jepsen.drivers.tidb
    (:require [clojure.tools.logging :refer [info warn]]
      [clojure [pprint :refer [pprint]]
       [string :as str]]
      [jepsen [cli :as cli]
       [checker :as checker]
       [db :as jdb]
       [generator :as gen]
       [os :as os]
       [tests :as tests]
       [nemesis :as jnemesis]
       [control :as c]
       [util :as util :refer [parse-long]]]
      [jepsen.os.debian :as debian]
      [jepsen.tidb [nemesis :as stolon-nemesis]]
      [jepsen.tidb.workloads
       [append :as append]
       ]))

(def workloads
  {
   :append append/workload
   ;  :ledger      ledger/workload
   :none        (fn [_] tests/noop-test)})

(def all-workloads
  "A collection of workloads we run by default."
  (remove #{:none} (keys workloads)))

(def workloads-expected-to-pass
  "A collection of workload names which we expect should actually pass."
  (remove #{} all-workloads))

(def short-isolation
  {
   :snapshot-isolation  "SI"
   })

(def all-nemeses
  "Combinations of nemeses for tests"
  [[]
   [:pause :kill :partition :clock]])

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none []
   :all  [:pause :kill :partition :clock]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(defn node-start-stopper
  "Takes a targeting function which, given a list of nodes, returns a single
  node or collection of nodes to affect, and two functions `(start! test node)`
  invoked on nemesis start, and `(stop! test node)` invoked on nemesis stop.
  Returns a nemesis which responds to :start and :stop by running the start!
  and stop! fns on each of the given nodes. During `start!` and `stop!`, binds
  the `jepsen.control` session to the given node, so you can just call `(c/exec
  ...)`.

  The targeter can take either (targeter test nodes) or, if that fails,
  (targeter nodes).

  Re-selects a fresh node (or nodes) for each start--if targeter returns nil,
  skips the start. The return values from the start and stop fns will become
  the :values of the returned :info operations from the nemesis, e.g.:

      {:value {:n1 [:killed \"java\"]}}"
  [targeter start! stop!]
  (let [nodes (atom nil)]
    (reify jnemesis/Nemesis
      (setup! [this test] this)

      (invoke! [this test op]
        (locking nodes
          (assoc op :type :info, :value
                 (case (:f op)
                   :start (let [ns (:nodes test)
                                ns "instance-4"
                                ns (util/coll ns)
                                _  (info (str "killing tikv-server" ns))]
                            (if ns
                              (if (compare-and-set! nodes nil ns)
                                (do (c/on-many ns (start! test c/*host*))
                                    (info ns))
                                (str "nemesis already disrupting "
                                     (pr-str @nodes)))
                              :no-target))
                   :stop (if-let [ns @nodes]
                           (let [value (c/on-many ns (stop! test c/*host*))]
                             (reset! nodes nil)
                             value)
                           :not-started)))))

      (teardown! [this test]))))


(defn hammer-time
  "Responds to `{:f :start}` by pausing the given process name on a given node
  or nodes using SIGSTOP, and when `{:f :stop}` arrives, resumes it with
  SIGCONT.  Picks the node(s) to pause using `(targeter list-of-nodes)`, which
  defaults to `rand-nth`. Targeter may return either a single node or a
  collection of nodes."
  ([process] (hammer-time rand-nth process))
  ([targeter process]
   (node-start-stopper targeter
                       (fn start [t n]
                         (c/su (c/exec :killall :-s "STOP" process))
                         [:paused process])
                       (fn stop [t n]
                         (c/su (c/exec :killall :-s "CONT" process))
                         [:resumed process]))))

(defn tidb-test
      "Given an options map from the command line runner (e.g. :nodes, :ssh,
      :concurrency, ...), constructs a test map."
      [opts]
      (print "in tidb-test: ")
      (print opts)
      (let [workload-name (:workload opts)
            workload      ((workloads workload-name) opts)
            db            (cond (:existing-tidb opts) jdb/noop)
            os            (if (:existing-tidb opts)
                            os/noop
                            debian/os)
            nemesis       (stolon-nemesis/nemesis-package
                            {:db        db
                             :nodes     (:nodes opts)
                             :faults    (:nemesis opts)
                             :partition {:targets [:primaries]}
                             :pause     {:targets [nil :one :primaries :majority :all]}
                             :kill      {:targets [nil :one :primaries :majority :all]}
                             :interval  (:nemesis-interval opts)})
            ]
           (merge tests/noop-test
                  opts
                  {:name (str "stolon " (name workload-name)
                              " " (short-isolation (:isolation opts)) " ("
                              (short-isolation (:expected-consistency-model opts)) ")"
                              " " (str/join "," (map name (:nemesis opts))))
                   :pure-generators true
                   :os   os
                   :db   db
                   :checker (checker/compose
                              {:perf       (checker/perf
                                             {:nemeses (:perf nemesis)})
                               :clock      (checker/clock-plot)
                               :stats      (checker/stats)
                               :exceptions (checker/unhandled-exceptions)
                               :workload   (:checker workload)})
                   :client    (:client workload)
                   :nemesis   (:nemesis nemesis)
                  ;  :nemesis (jnemesis/partition-random-halves)
                  ; :nemesis (jnemesis/clock-scrambler 5) ; clock sket in a dt-second window
                  ; :nemesis (jnemesis/hammer-time "/tidb-deploy/tikv-20160/bin/tikv-server") ; pause and resume a given process: /tidb-deploy/tikv-20160/bin
                  ;; :nemesis (hammer-time "/tidb-deploy/tikv-20160/bin/tikv-server") ; pause and resume a given process: /tidb-deploy/tikv-20160/bin
                  ;; :nemesis (hammer-time "/tidb-deploy/pd-2379/bin/pd-server") ; pause and resume a given process: /tidb-deploy/tikv-20160/bin

                  ;;  :generator (gen/phases
                  ;;               (gen/nemesis {:type :info, :f :stop})
                  ;;               (->> (:generator workload)
                  ;;                    (gen/stagger (/ (:rate opts)))
                  ;;                    (gen/nemesis
                  ;;                       (cycle [(gen/sleep 2)
                  ;;                               {:type :info, :f :start}
                  ;;                               (gen/sleep 5)
                  ;;                               {:type :info, :f :stop}
                  ;;                               ]))
                  ;;                    (gen/time-limit (:time-limit opts)))
                  ;;               (gen/nemesis {:type :info, :f :stop})
                  ;;               )

                  :generator (gen/phases
                                (->> (:generator workload)
                                     (gen/stagger (/ (:rate opts)))
                                     (gen/nemesis (:generator nemesis))
                                     (gen/time-limit (:time-limit opts))))})))
                                    
(def cli-opts
  "Additional CLI options"
  [[nil "--etcd-version STRING" "What version of etcd should we install?"
    :default "3.4.3"]

   ["-i" "--isolation LEVEL" "What level of isolation we should set: serializable, repeatable-read, etc."
    :default :snapshot-isolation
    :parse-fn keyword
    :validate [#{:snapshot-isolation
                 }
               "Should be one of read-uncommitted, read-committed, repeatable-read, or serializable"]]

   [nil "--existing-tidb" "If set, assumes nodes already have a running tidb instance, skipping any OS and DB setup and teardown. Suitable for debugging issues against a local instance of tidb (or some sort of pre-built cluster) when you don't want to set up a whole-ass Jepsen environment."
    :default false]

   [nil "--expected-consistency-model MODEL" "What level of isolation do we *expect* to observe? Defaults to the same as --isolation."
    :default nil
    :parse-fn keyword]

   [nil "--key-count NUM" "Number of keys in active rotation."
    :default  10
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? #{:pause :kill :partition :clock :member})
               "Faults must be pause, kill, partition, clock, or member, or the special faults all or none."]]

   [nil "--max-txn-length NUM" "Maximum number of operations in a transaction."
    :default  4
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM" "Maximum number of writes to any given key."
    :default  256
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default 5
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--dbname STRING" "the database name"
    :default "mys"] ; you'd better specify your password in lein statemen

   [nil "--tidb-password PASS" "What password should we use to connect to postgres?"
    :default ""] ; you'd better specify your password in lein statemen

   [nil "--tidb-port NUMBER" "What port should we connect to when talking to postgres?"
    :default 4000
    :parse-fn parse-long]

   [nil "--tidb-user NAME" "What username should we use to connect to postgres? Only use this with --existing-postgres, or you'll probably confuse the Stolon setup."
    :default "root"]

   [nil "--prepare-threshold INT" "Passes a prepareThreshold option to the JDBC spec."
    :parse-fn parse-long]

   ["-r" "--rate HZ" "Approximate request rate, in hz"
    :default 100
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   ["-v" "--version STRING" "What version of Stolon should we test?"
    :default "0.16.0"]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]

   [nil "--downgrade" "If set, we downdrage insertion to update, deletion to nothing when applicable"
    :default false]
   ])


(defn opt-fn
      "Transforms CLI options before execution."
      [parsed]
      (update-in parsed [:options :expected-consistency-model]
                 #(or % (get-in parsed [:options :isolation]))))

(defn -main
      "Handles command line arguments. Can either run a test, or a web server for
      browsing results."
      [& args]
      (cli/run! (merge (cli/single-test-cmd {:test-fn  tidb-test
                                             :opt-spec cli-opts
                                             :opt-fn   opt-fn})
                       ;(cli/serve-cmd)
                       )
                args))
