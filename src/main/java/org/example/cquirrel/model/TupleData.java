package org.example.cquirrel.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TupleData implements Serializable {

    private String relationName;
    private String primaryKey;
    private Map<String, String> attributes = new HashMap<>();

    public TupleData() {
    }

    public TupleData(String relationName, String primaryKey, Map<String, String> attributes) {
        this.relationName = relationName;
        this.primaryKey = primaryKey;
        this.attributes = new HashMap<>(attributes);
    }

    public String getRelationName() {
        return relationName;
    }

    public void setRelationName(String relationName) {
        this.relationName = relationName;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = new HashMap<>(attributes);
    }

    @Override
    public String toString() {
        return "TupleData{"
                + "relationName='" + relationName + '\''
                + ", primaryKey='" + primaryKey + '\''
                + ", attributes=" + attributes
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TupleData)) {
            return false;
        }
        TupleData tupleData = (TupleData) o;
        return Objects.equals(relationName, tupleData.relationName)
                && Objects.equals(primaryKey, tupleData.primaryKey)
                && Objects.equals(attributes, tupleData.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(relationName, primaryKey, attributes);
    }
}
