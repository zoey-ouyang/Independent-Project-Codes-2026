package org.example.cquirrel.result;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class JoinAnswer implements Serializable {

    private final Map<String, String> tupleKeysByRelation;
    private final Map<String, String> projectedValues;

    public JoinAnswer(Map<String, String> tupleKeysByRelation, Map<String, String> projectedValues) {
        this.tupleKeysByRelation = new LinkedHashMap<>(tupleKeysByRelation);
        this.projectedValues = new LinkedHashMap<>(projectedValues);
    }

    public Map<String, String> getTupleKeysByRelation() {
        return tupleKeysByRelation;
    }

    public Map<String, String> getProjectedValues() {
        return projectedValues;
    }

    public String projectedValue(String columnRef) {
        return projectedValues.get(columnRef);
    }

    public String groupKey(String groupByColumn) {
        return projectedValues.getOrDefault(groupByColumn, "");
    }

    @Override
    public String toString() {
        return "JoinAnswer{"
                + "tupleKeysByRelation=" + tupleKeysByRelation
                + ", projectedValues=" + projectedValues
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JoinAnswer)) {
            return false;
        }
        JoinAnswer that = (JoinAnswer) o;
        return Objects.equals(tupleKeysByRelation, that.tupleKeysByRelation)
                && Objects.equals(projectedValues, that.projectedValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tupleKeysByRelation, projectedValues);
    }
}
