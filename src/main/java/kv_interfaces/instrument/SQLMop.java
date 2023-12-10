package kv_interfaces.instrument;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import kv_interfaces.OP_TYPE;

class SQLMop {
    public String opType = null;
    public String sql = null;
    public Object[] v2s = null;

    public SQLMop(OP_TYPE op_type, String sql, Object[] v2s){
        Map<OP_TYPE, String> op2str = Map.of(
            OP_TYPE.START_TXN, "begin",
            OP_TYPE.ABORT_TXN, "abort",
            OP_TYPE.COMMIT_TXN, "commit",
            OP_TYPE.READ, "SELECT",
            OP_TYPE.WRITE, "UPDATE",
            OP_TYPE.INSERT, "INSERT",
            OP_TYPE.DELETE, "DELETE"
        );

        this.opType = op2str.get(op_type);
        this.sql = sql;
        this.v2s = v2s;
    }

    public String toString(){
        ObjectMapper mapper = new ObjectMapper();
        String json = null;
        try {
            json = mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return json;
    }
}