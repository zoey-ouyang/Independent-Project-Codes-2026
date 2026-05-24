package org.example.cquirrel.query;

import java.io.Serializable;
import java.util.Objects;

public class JoinEdge implements Serializable {

    private String leftRelation;
    private String leftColumn;
    private String rightRelation;
    private String rightColumn;

    public JoinEdge() {
    }

    public JoinEdge(String leftRelation, String leftColumn, String rightRelation, String rightColumn) {
        this.leftRelation = leftRelation;
        this.leftColumn = leftColumn;
        this.rightRelation = rightRelation;
        this.rightColumn = rightColumn;
    }

    public String getLeftRelation() {
        return leftRelation;
    }

    public String getLeftColumn() {
        return leftColumn;
    }

    public String getRightRelation() {
        return rightRelation;
    }

    public String getRightColumn() {
        return rightColumn;
    }

    @Override
    public String toString() {
        return leftRelation + "." + leftColumn + " = "
                + rightRelation + "." + rightColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JoinEdge)) {
            return false;
        }
        JoinEdge joinEdge = (JoinEdge) o;
        return Objects.equals(leftRelation, joinEdge.leftRelation)
                && Objects.equals(leftColumn, joinEdge.leftColumn)
                && Objects.equals(rightRelation, joinEdge.rightRelation)
                && Objects.equals(rightColumn, joinEdge.rightColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leftRelation, leftColumn, rightRelation, rightColumn);
    }
}
