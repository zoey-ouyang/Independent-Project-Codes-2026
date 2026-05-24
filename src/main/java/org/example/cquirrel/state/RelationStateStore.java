package org.example.cquirrel.state;

import org.example.cquirrel.model.RelationUpdateResult;
import org.example.cquirrel.model.TupleData;
import org.example.cquirrel.model.UpdateEvent;
import org.example.cquirrel.model.UpdateOperation;
import org.example.cquirrel.query.AggregationSpec;
import org.example.cquirrel.query.QueryPlan;
import org.example.cquirrel.result.AggregateRow;
import org.example.cquirrel.result.AnswerDelta;
import org.example.cquirrel.result.JoinAnswer;
import org.example.cquirrel.schema.ForeignKey;
import org.example.cquirrel.schema.RelationSchema;
import org.example.cquirrel.schema.SchemaGraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RelationStateStore implements Serializable {

    private final SchemaGraph schemaGraph;
    private final QueryPlan queryPlan;
    private final Map<String, Map<String, TupleData>> tuplesByRelation = new LinkedHashMap<>();
    private final Map<String, Map<String, Map<String, Set<String>>>> fkIndexByRelation = new LinkedHashMap<>();
    private final Map<String, Set<String>> liveKeysByRelation = new LinkedHashMap<>();
    private final List<AggregationSpec> aggregationSpecs;
    private Set<JoinAnswer> currentAnswers = new LinkedHashSet<>();
    private Map<String, AggregateRow> aggregateRows = new LinkedHashMap<>();

    public RelationStateStore(SchemaGraph schemaGraph, QueryPlan queryPlan) {
        this.schemaGraph = schemaGraph;
        this.queryPlan = queryPlan;
        this.queryPlan.validateAgainst(schemaGraph);
        this.aggregationSpecs = queryPlan.aggregationSpecs();
        for (RelationSchema schema : schemaGraph.getRelations().values()) {
            tuplesByRelation.put(schema.getName(), new LinkedHashMap<>());
            liveKeysByRelation.put(schema.getName(), new LinkedHashSet<>());
            fkIndexByRelation.put(schema.getName(), new LinkedHashMap<>());
            for (ForeignKey foreignKey : schema.getForeignKeys()) {
                fkIndexByRelation.get(schema.getName()).put(foreignKey.getSourceColumn(), new LinkedHashMap<>());
            }
        }
    }

    public RelationUpdateResult apply(UpdateEvent event) {
        RelationSchema relationSchema = schemaGraph.getRelations().get(event.getRelation());
        if (relationSchema == null) {
            throw new IllegalArgumentException("Unknown relation in update event: " + event.getRelation());
        }

        Map<String, TupleData> relationTuples = tuplesByRelation.get(event.getRelation());
        TupleData previousTuple = relationTuples.get(event.getPrimaryKey());

        if (event.getOp() == UpdateOperation.DELETE) {
            if (previousTuple != null) {
                removeForeignKeyIndexEntries(relationSchema, previousTuple);
                relationTuples.remove(event.getPrimaryKey());
            }
        } else {
            if (event.getPayload() == null) {
                throw new IllegalArgumentException("Insert/update event requires a payload: " + event);
            }
            if (previousTuple != null) {
                removeForeignKeyIndexEntries(relationSchema, previousTuple);
            }
            relationTuples.put(event.getPrimaryKey(), event.getPayload());
            addForeignKeyIndexEntries(relationSchema, event.getPayload());
        }

        recomputeLiveTuples();
        Set<JoinAnswer> nextAnswers = enumerateCurrentAnswers();
        List<AnswerDelta> answerDeltas = diffAnswers(currentAnswers, nextAnswers);
        currentAnswers = nextAnswers;
        aggregateRows = aggregateByGroup(currentAnswers);

        int relationTupleCount = relationTuples.size();
        int indexedForeignKeyValueCount = countIndexedForeignKeyValues(event.getRelation());
        int liveTupleCount = countLiveTuples();
        String summary = buildSummary(
                event,
                previousTuple,
                relationTupleCount,
                indexedForeignKeyValueCount,
                liveTupleCount,
                answerDeltas.size()
        );
        return new RelationUpdateResult(
                event,
                relationTupleCount,
                indexedForeignKeyValueCount,
                liveTupleCount,
                answerDeltas,
                aggregateRows,
                summary
        );
    }

    private void addForeignKeyIndexEntries(RelationSchema relationSchema, TupleData tupleData) {
        for (ForeignKey foreignKey : relationSchema.getForeignKeys()) {
            String fkValue = tupleData.getAttributes().get(foreignKey.getSourceColumn());
            if (fkValue == null) {
                continue;
            }
            fkIndexByRelation.get(relationSchema.getName())
                    .get(foreignKey.getSourceColumn())
                    .computeIfAbsent(fkValue, ignored -> new LinkedHashSet<>())
                    .add(tupleData.getPrimaryKey());
        }
    }

    private void removeForeignKeyIndexEntries(RelationSchema relationSchema, TupleData tupleData) {
        for (ForeignKey foreignKey : relationSchema.getForeignKeys()) {
            String fkValue = tupleData.getAttributes().get(foreignKey.getSourceColumn());
            if (fkValue == null) {
                continue;
            }

            Map<String, Set<String>> valueIndex =
                    fkIndexByRelation.get(relationSchema.getName()).get(foreignKey.getSourceColumn());
            Set<String> primaryKeys = valueIndex.get(fkValue);
            if (primaryKeys == null) {
                continue;
            }
            primaryKeys.remove(tupleData.getPrimaryKey());
            if (primaryKeys.isEmpty()) {
                valueIndex.remove(fkValue);
            }
        }
    }

    private void recomputeLiveTuples() {
        for (Set<String> liveKeys : liveKeysByRelation.values()) {
            liveKeys.clear();
        }

        for (String relation : schemaGraph.topologicalOrder()) {
            RelationSchema relationSchema = schemaGraph.getRelations().get(relation);
            for (TupleData tuple : tuplesByRelation.get(relation).values()) {
                if (allParentsAreLive(relationSchema, tuple)) {
                    liveKeysByRelation.get(relation).add(tuple.getPrimaryKey());
                }
            }
        }
    }

    private boolean allParentsAreLive(RelationSchema relationSchema, TupleData tuple) {
        for (ForeignKey fk : relationSchema.getForeignKeys()) {
            String parentKey = tuple.getAttributes().get(fk.getSourceColumn());
            if (parentKey == null || !liveKeysByRelation.get(fk.getTargetRelation()).contains(parentKey)) {
                return false;
            }
        }
        return true;
    }

    private Set<JoinAnswer> enumerateCurrentAnswers() {
        Set<JoinAnswer> answers = new LinkedHashSet<>();
        List<String> orderedQueryRelations = orderedQueryRelations();
        enumerateByTopologicalOrder(orderedQueryRelations, 0, new LinkedHashMap<String, TupleData>(), answers);
        return answers;
    }

    private List<String> orderedQueryRelations() {
        Set<String> queryRelations = new LinkedHashSet<>(queryPlan.getRelations());
        List<String> ordered = new ArrayList<>();
        for (String relation : schemaGraph.topologicalOrder()) {
            if (queryRelations.contains(relation)) {
                ordered.add(relation);
            }
        }
        return ordered;
    }

    private void enumerateByTopologicalOrder(
            List<String> orderedQueryRelations,
            int relationIndex,
            Map<String, TupleData> partial,
            Set<JoinAnswer> answers
    ) {
        if (relationIndex == orderedQueryRelations.size()) {
            answers.add(toJoinAnswer(partial));
            return;
        }

        String relation = orderedQueryRelations.get(relationIndex);
        for (String candidateKey : candidateLiveKeys(relation, partial)) {
            TupleData tuple = tuplesByRelation.get(relation).get(candidateKey);
            if (tuple == null) {
                continue;
            }
            partial.put(relation, tuple);
            enumerateByTopologicalOrder(orderedQueryRelations, relationIndex + 1, partial, answers);
            partial.remove(relation);
        }
    }

    private List<String> candidateLiveKeys(String relation, Map<String, TupleData> partial) {
        List<String> matches = null;
        for (String parentRelation : queryPlan.getRelations()) {
            if (!partial.containsKey(parentRelation)) {
                continue;
            }
            if (schemaGraph.foreignKeysFromChildToParent(relation, parentRelation).isEmpty()) {
                continue;
            }
            List<String> parentMatches = matchingLiveChildKeys(parentRelation, relation, partial.get(parentRelation));
            if (matches == null) {
                matches = parentMatches;
            } else {
                matches.retainAll(parentMatches);
            }
        }

        if (matches != null) {
            return matches;
        }
        return new ArrayList<>(liveKeysByRelation.get(relation));
    }

    private List<String> matchingLiveChildKeys(String parentRelation, String childRelation, TupleData parentTuple) {
        List<ForeignKey> foreignKeys = schemaGraph.foreignKeysFromChildToParent(childRelation, parentRelation);
        if (foreignKeys.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> matches = null;
        for (ForeignKey fk : foreignKeys) {
            String parentValue = parentTuple.getAttributes().get(fk.getTargetColumn());
            Set<String> keys = fkIndexByRelation.get(childRelation)
                    .get(fk.getSourceColumn())
                    .getOrDefault(parentValue, Collections.emptySet());
            Set<String> liveKeys = liveKeysByRelation.get(childRelation);
            List<String> liveMatches = new ArrayList<>();
            for (String key : keys) {
                if (liveKeys.contains(key)) {
                    liveMatches.add(key);
                }
            }
            if (matches == null) {
                matches = liveMatches;
            } else {
                matches.retainAll(liveMatches);
            }
        }
        return matches == null ? Collections.emptyList() : matches;
    }

    private JoinAnswer toJoinAnswer(Map<String, TupleData> partial) {
        Map<String, String> tupleKeys = new LinkedHashMap<>();
        Map<String, String> projectedValues = new LinkedHashMap<>();

        for (String relation : queryPlan.getRelations()) {
            TupleData tuple = partial.get(relation);
            tupleKeys.put(relation, tuple.getPrimaryKey());
        }

        for (String columnRef : queryPlan.requiredValueColumns()) {
            String[] parts = columnRef.split("\\.", 2);
            if (parts.length != 2) {
                continue;
            }
            TupleData tuple = partial.get(parts[0]);
            if (tuple != null) {
                projectedValues.put(columnRef, tuple.getAttributes().get(parts[1]));
            }
        }

        return new JoinAnswer(tupleKeys, projectedValues);
    }

    private List<AnswerDelta> diffAnswers(Set<JoinAnswer> previous, Set<JoinAnswer> next) {
        List<AnswerDelta> deltas = new ArrayList<>();
        for (JoinAnswer answer : next) {
            if (!previous.contains(answer)) {
                deltas.add(new AnswerDelta(answer, 1));
            }
        }
        for (JoinAnswer answer : previous) {
            if (!next.contains(answer)) {
                deltas.add(new AnswerDelta(answer, -1));
            }
        }
        return deltas;
    }

    private Map<String, AggregateRow> aggregateByGroup(Set<JoinAnswer> answers) {
        Map<String, AggregateRow> grouped = new LinkedHashMap<>();
        for (JoinAnswer answer : answers) {
            String groupKey = groupKeyOf(answer);
            AggregateRow row = grouped.computeIfAbsent(groupKey, ignored -> new AggregateRow());
            if (aggregationSpecs.isEmpty()) {
                row.incrementCount();
                continue;
            }

            for (AggregationSpec aggregationSpec : aggregationSpecs) {
                if (aggregationSpec.getType() == AggregationSpec.Type.COUNT) {
                    row.incrementCount();
                } else if (aggregationSpec.getType() == AggregationSpec.Type.SUM) {
                    String value = answer.projectedValue(aggregationSpec.requiredProjection());
                    if (value != null && !value.trim().isEmpty()) {
                        row.addSum(aggregationSpec.getExpression(), Double.parseDouble(value));
                    }
                }
            }
        }
        return grouped;
    }

    private String groupKeyOf(JoinAnswer answer) {
        if (queryPlan.getGroupByColumns().isEmpty()) {
            return "__all__";
        }

        List<String> values = new ArrayList<>();
        for (String column : queryPlan.getGroupByColumns()) {
            values.add(answer.groupKey(column));
        }
        return String.join("|", values);
    }

    private int countIndexedForeignKeyValues(String relationName) {
        int count = 0;
        for (Map<String, Set<String>> valueIndex : fkIndexByRelation.get(relationName).values()) {
            count += valueIndex.size();
        }
        return count;
    }

    private int countLiveTuples() {
        int count = 0;
        for (Set<String> keys : liveKeysByRelation.values()) {
            count += keys.size();
        }
        return count;
    }

    private String buildSummary(
            UpdateEvent event,
            TupleData previousTuple,
            int relationTupleCount,
            int indexedForeignKeyValueCount,
            int liveTupleCount,
            int deltaCount
    ) {
        String changeDescription;
        if (event.getOp() == UpdateOperation.DELETE) {
            changeDescription = previousTuple == null ? "delete-miss" : "deleted";
        } else if (previousTuple == null) {
            changeDescription = "inserted";
        } else {
            changeDescription = "updated";
        }

        return "relation=" + event.getRelation()
                + ", key=" + event.getPrimaryKey()
                + ", change=" + changeDescription
                + ", relationTupleCount=" + relationTupleCount
                + ", indexedForeignKeyValueCount=" + indexedForeignKeyValueCount
                + ", liveTupleCount=" + liveTupleCount
                + ", answerDeltaCount=" + deltaCount
                + ", aggregateRows=" + aggregateRows;
    }
}
