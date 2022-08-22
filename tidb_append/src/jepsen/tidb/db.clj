(ns jepsen.tidb.db
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clj-http.client :as http]
            [cheshire.core :as json]
            [dom-top.core :refer [with-retry]]
            [fipp.edn :refer [pprint]]
            [jepsen [core :as jepsen]
                    [control :as c]
                    [db :as db]
                    [faketime :as faketime]
                    [util :as util]]
            [jepsen.control.util :as cu]
            [slingshot.slingshot :refer [try+ throw+]]
            [tidb.sql :as sql]))

(def replica-count
  "This should probably be used to generate max-replicas in pd.conf as well,
  but for now we'll just write it in both places."
  3)


(def client-port 2379)
(def peer-port   2380)

(defn tidb-map
  "Computes node IDs for a test."
  [test]
  (->> (:nodes test)
       (map-indexed (fn [i node]
                      [node {:pd (str "pd" (inc i))
                             :kv (str "kv" (inc i))}]))
       (into {})))

(defn node-url
  "An HTTP url for connecting to a node on a particular port."
  [node port]
  (str "http://" (name node) ":" port))


(defn peer-url
  "The HTTP url for other peers to talk to a node."
  [node]
  (node-url node peer-port))

(defn initial-cluster
  "Constructs an initial cluster string for a test, like
  \"foo=foo:2380,bar=bar:2380,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node] (str (get-in (tidb-map test) [node :pd])
                            "=" (peer-url node))))
       (str/join ",")))




(defn page-ready?
  "Fetches a status page URL on the local node, and returns true iff the page
  was available."
  [url]
  (try+
    (c/exec :curl :--fail url)
    (catch [:type :jepsen.control/nonzero-exit] _ false)))




(defn region-ready?
  "Does the given region have enough replicas?"
  [region]
  (->> (:peers region)
       (remove :is_learner)
       count
       (<= replica-count)))

