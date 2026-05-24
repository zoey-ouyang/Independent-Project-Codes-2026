package org.example.cquirrel.schema;

import java.io.Serializable;
import java.util.Objects;

public class ForeignKey implements Serializable {

    private String sourceColumn;
    private String targetRelation;
    private String targetColumn;

    public ForeignKey() {
    }

    public ForeignKey(String sourceColumn, String targetRelation, String targetColumn) {
        this.sourceColumn = sourceColumn;
        this.targetRelation = targetRelation;
        this.targetColumn = targetColumn;
    }

    public String getSourceColumn() {
        return sourceColumn;
    }

    public String getTargetRelation() {
        return targetRelation;
    }

    public String getTargetColumn() {
        return targetColumn;
    }

    @Override
    public String toString() {
        return sourceColumn + " -> " + targetRelation + "." + targetColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ForeignKey)) {
            return false;
        }
        ForeignKey that = (ForeignKey) o;
        return Objects.equals(sourceColumn, that.sourceColumn)
                && Objects.equals(targetRelation, that.targetRelation)
                && Objects.equals(targetColumn, that.targetColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceColumn, targetRelation, targetColumn);
    }
}
