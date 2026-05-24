package org.example.cquirrel.model;

import java.io.Serializable;
import java.util.Objects;

public class UpdateEvent implements Serializable {

    private UpdateOperation op;
    private String relation;
    private String primaryKey;
    private TupleData payload;
    private long timestamp;

    public UpdateEvent() {
    }

    public UpdateEvent(UpdateOperation op, String relation, String primaryKey, TupleData payload, long timestamp) {
        this.op = op;
        this.relation = relation;
        this.primaryKey = primaryKey;
        this.payload = payload;
        this.timestamp = timestamp;
    }

    public UpdateOperation getOp() {
        return op;
    }

    public void setOp(UpdateOperation op) {
        this.op = op;
    }

    public String getRelation() {
        return relation;
    }

    public void setRelation(String relation) {
        this.relation = relation;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public TupleData getPayload() {
        return payload;
    }

    public void setPayload(TupleData payload) {
        this.payload = payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UpdateEvent{"
                + "op=" + op
                + ", relation='" + relation + '\''
                + ", primaryKey='" + primaryKey + '\''
                + ", payload=" + payload
                + ", timestamp=" + timestamp
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UpdateEvent)) {
            return false;
        }
        UpdateEvent that = (UpdateEvent) o;
        return timestamp == that.timestamp
                && op == that.op
                && Objects.equals(relation, that.relation)
                && Objects.equals(primaryKey, that.primaryKey)
                && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, relation, primaryKey, payload, timestamp);
    }
}
