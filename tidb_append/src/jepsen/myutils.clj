(ns jepsen.myutils)

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "txn" i))

(defn table-for
  "What table should we use for the given key?
  k -> table index"
  [table-count k]
  (table-name (mod (hash k) table-count)))

