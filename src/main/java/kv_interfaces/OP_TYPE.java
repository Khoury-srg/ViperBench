package kv_interfaces;

public 	enum OP_TYPE {
    START_TXN, READ, WRITE, COMMIT_TXN, ABORT_TXN, INSERT, DELETE, RANGE, RANGE_INSERT, RANGE_DELETE, FINAL, INITIAL
};
