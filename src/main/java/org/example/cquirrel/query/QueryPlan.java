package org.example.cquirrel.query;

import org.example.cquirrel.schema.ForeignKey;
import org.example.cquirrel.schema.SchemaGraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryPlan implements Serializable {

    private final String name;
    private final List<String> relations = new ArrayList<>();
    private final List<JoinEdge> joinEdges = new ArrayList<>();
    private final List<String> projections = new ArrayList<>();
    private final List<String> groupByColumns = new ArrayList<>();
    private final List<String> aggregations = new ArrayList<>();

    public QueryPlan(String name) {
        this.name = name;
    }

    public void addRelation(String relation) {
        relations.add(relation);
    }

    public void addJoinEdge(JoinEdge joinEdge) {
        joinEdges.add(joinEdge);
    }

    public void addProjection(String projection) {
        projections.add(projection);
    }

    public void addGroupBy(String groupByColumn) {
        groupByColumns.add(groupByColumn);
    }

    public void addAggregation(String aggregation) {
        aggregations.add(aggregation);
    }

    public String getName() {
        return name;
    }

    public List<String> getRelations() {
        return relations;
    }

    public List<JoinEdge> getJoinEdges() {
        return joinEdges;
    }

    public List<String> getProjections() {
        return projections;
    }

    public List<String> getGroupByColumns() {
        return groupByColumns;
    }

    public List<String> getAggregations() {
        return aggregations;
    }

    public List<AggregationSpec> aggregationSpecs() {
        List<AggregationSpec> specs = new ArrayList<>();
        for (String aggregation : aggregations) {
            specs.add(AggregationSpec.parse(aggregation));
        }
        return specs;
    }

    public List<String> requiredValueColumns() {
        List<String> columns = new ArrayList<>(projections);
        for (AggregationSpec spec : aggregationSpecs()) {
            String requiredProjection = spec.requiredProjection();
            if (requiredProjection != null && !columns.contains(requiredProjection)) {
                columns.add(requiredProjection);
            }
        }
        return columns;
    }

    public void validateAgainst(SchemaGraph schemaGraph) {
        for (String relation : relations) {
            if (!schemaGraph.getRelations().containsKey(relation)) {
                throw new IllegalArgumentException("Query references unknown relation: " + relation);
            }
        }

        for (JoinEdge joinEdge : joinEdges) {
            boolean matchesForwardFk = matchesForeignKey(
                    schemaGraph,
                    joinEdge.getLeftRelation(),
                    joinEdge.getLeftColumn(),
                    joinEdge.getRightRelation(),
                    joinEdge.getRightColumn()
            );
            boolean matchesReverseFk = matchesForeignKey(
                    schemaGraph,
                    joinEdge.getRightRelation(),
                    joinEdge.getRightColumn(),
                    joinEdge.getLeftRelation(),
                    joinEdge.getLeftColumn()
            );
            if (!matchesForwardFk && !matchesReverseFk) {
                throw new IllegalArgumentException(
                        "Join edge is not backed by a schema foreign key: " + joinEdge
                );
            }
        }

        aggregationSpecs();
    }

    private boolean matchesForeignKey(
            SchemaGraph schemaGraph,
            String parentRelation,
            String parentColumn,
            String childRelation,
            String childColumn
    ) {
        List<ForeignKey> foreignKeys = schemaGraph.foreignKeysFromChildToParent(childRelation, parentRelation);
        for (ForeignKey foreignKey : foreignKeys) {
            if (foreignKey.getSourceColumn().equals(childColumn)
                    && foreignKey.getTargetColumn().equals(parentColumn)) {
                return true;
            }
        }
        return false;
    }

    public List<String> getRelationsView() {
        return Collections.unmodifiableList(relations);
    }

    @Override
    public String toString() {
        return "QueryPlan{"
                + "name='" + name + '\''
                + ", relations=" + relations
                + ", joinEdges=" + joinEdges
                + ", projections=" + projections
                + ", groupByColumns=" + groupByColumns
                + ", aggregations=" + aggregations
                + '}';
    }
}
