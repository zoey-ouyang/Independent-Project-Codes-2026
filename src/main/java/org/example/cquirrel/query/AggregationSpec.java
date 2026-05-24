package org.example.cquirrel.query;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;

public class AggregationSpec implements Serializable {

    public enum Type {
        COUNT,
        SUM
    }

    private final String expression;
    private final Type type;
    private final String relation;
    private final String column;

    private AggregationSpec(String expression, Type type, String relation, String column) {
        this.expression = expression;
        this.type = type;
        this.relation = relation;
        this.column = column;
    }

    public static AggregationSpec parse(String expression) {
        String normalized = expression.trim();
        String upper = normalized.toUpperCase(Locale.ROOT);
        if ("COUNT(*)".equals(upper)) {
            return new AggregationSpec(normalized, Type.COUNT, null, null);
        }

        if (upper.startsWith("SUM(") && normalized.endsWith(")")) {
            String body = normalized.substring(4, normalized.length() - 1).trim();
            String[] parts = body.split("\\.", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException("SUM aggregation must use relation.column syntax: " + expression);
            }
            return new AggregationSpec(normalized, Type.SUM, parts[0], parts[1]);
        }

        throw new IllegalArgumentException("Unsupported aggregation: " + expression);
    }

    public String requiredProjection() {
        if (type != Type.SUM) {
            return null;
        }
        return relation + "." + column;
    }

    public String getExpression() {
        return expression;
    }

    public Type getType() {
        return type;
    }

    public String getRelation() {
        return relation;
    }

    public String getColumn() {
        return column;
    }

    @Override
    public String toString() {
        return expression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AggregationSpec)) {
            return false;
        }
        AggregationSpec that = (AggregationSpec) o;
        return Objects.equals(expression, that.expression)
                && type == that.type
                && Objects.equals(relation, that.relation)
                && Objects.equals(column, that.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, type, relation, column);
    }
}
