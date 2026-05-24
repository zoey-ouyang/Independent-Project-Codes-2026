package org.example.cquirrel.result;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class AggregateRow implements Serializable {

    private long count;
    private final Map<String, Double> sums = new LinkedHashMap<>();

    public void incrementCount() {
        count += 1L;
    }

    public void addSum(String expression, double delta) {
        sums.merge(expression, delta, Double::sum);
    }

    public long getCount() {
        return count;
    }

    public Map<String, Double> getSums() {
        return sums;
    }

    @Override
    public String toString() {
        return "AggregateRow{"
                + "count=" + count
                + ", sums=" + sums
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AggregateRow)) {
            return false;
        }
        AggregateRow that = (AggregateRow) o;
        return count == that.count && Objects.equals(sums, that.sums);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, sums);
    }
}
