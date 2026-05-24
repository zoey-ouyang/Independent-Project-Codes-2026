package org.example.cquirrel.model;

import org.example.cquirrel.result.AnswerDelta;
import org.example.cquirrel.result.AggregateRow;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.io.Serializable;
import java.util.Objects;

public class RelationUpdateResult implements Serializable {

    private UpdateEvent event;
    private int relationTupleCount;
    private int indexedForeignKeyValueCount;
    private int liveTupleCount;
    private List<AnswerDelta> answerDeltas = new ArrayList<>();
    private Map<String, AggregateRow> aggregateRows = new LinkedHashMap<>();
    private String summary;

    public RelationUpdateResult() {
    }

    public RelationUpdateResult(
            UpdateEvent event,
            int relationTupleCount,
            int indexedForeignKeyValueCount,
            int liveTupleCount,
            List<AnswerDelta> answerDeltas,
            Map<String, AggregateRow> aggregateRows,
            String summary
    ) {
        this.event = event;
        this.relationTupleCount = relationTupleCount;
        this.indexedForeignKeyValueCount = indexedForeignKeyValueCount;
        this.liveTupleCount = liveTupleCount;
        this.answerDeltas = new ArrayList<>(answerDeltas);
        this.aggregateRows = new LinkedHashMap<>(aggregateRows);
        this.summary = summary;
    }

    public UpdateEvent getEvent() {
        return event;
    }

    public int getRelationTupleCount() {
        return relationTupleCount;
    }

    public int getIndexedForeignKeyValueCount() {
        return indexedForeignKeyValueCount;
    }

    public int getLiveTupleCount() {
        return liveTupleCount;
    }

    public List<AnswerDelta> getAnswerDeltas() {
        return answerDeltas;
    }

    public Map<String, AggregateRow> getAggregateRows() {
        return aggregateRows;
    }

    public String getSummary() {
        return summary;
    }

    @Override
    public String toString() {
        return "RelationUpdateResult{"
                + "event=" + event
                + ", relationTupleCount=" + relationTupleCount
                + ", indexedForeignKeyValueCount=" + indexedForeignKeyValueCount
                + ", liveTupleCount=" + liveTupleCount
                + ", answerDeltas=" + answerDeltas
                + ", aggregateRows=" + aggregateRows
                + ", summary='" + summary + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RelationUpdateResult)) {
            return false;
        }
        RelationUpdateResult that = (RelationUpdateResult) o;
        return relationTupleCount == that.relationTupleCount
                && indexedForeignKeyValueCount == that.indexedForeignKeyValueCount
                && liveTupleCount == that.liveTupleCount
                && Objects.equals(event, that.event)
                && Objects.equals(answerDeltas, that.answerDeltas)
                && Objects.equals(aggregateRows, that.aggregateRows)
                && Objects.equals(summary, that.summary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                event,
                relationTupleCount,
                indexedForeignKeyValueCount,
                liveTupleCount,
                answerDeltas,
                aggregateRows,
                summary
        );
    }
}
