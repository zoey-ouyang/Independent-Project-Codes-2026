package org.example.cquirrel.schema;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class SchemaGraph implements Serializable {

    private final Map<String, RelationSchema> relations = new LinkedHashMap<>();
    private final Map<String, List<String>> adjacency = new LinkedHashMap<>();

    public static SchemaGraph fromSchemas(List<RelationSchema> schemas) {
        SchemaGraph graph = new SchemaGraph();
        for (RelationSchema schema : schemas) {
            graph.addRelation(schema);
        }
        graph.validateAcyclic();
        return graph;
    }

    public void addRelation(RelationSchema schema) {
        relations.put(schema.getName(), schema);
        adjacency.putIfAbsent(schema.getName(), new ArrayList<>());
        for (ForeignKey fk : schema.getForeignKeys()) {
            adjacency.computeIfAbsent(fk.getTargetRelation(), key -> new ArrayList<>());
            adjacency.get(fk.getTargetRelation()).add(schema.getName());
        }
    }

    public void validateForeignKeys() {
        for (RelationSchema schema : relations.values()) {
            for (ForeignKey fk : schema.getForeignKeys()) {
                RelationSchema target = relations.get(fk.getTargetRelation());
                if (target == null) {
                    throw new IllegalArgumentException(
                            "Foreign key from " + schema.getName() + "." + fk.getSourceColumn()
                                    + " references unknown relation " + fk.getTargetRelation()
                    );
                }
                if (!target.getPrimaryKeyColumns().contains(fk.getTargetColumn())) {
                    throw new IllegalArgumentException(
                            "Foreign key from " + schema.getName() + "." + fk.getSourceColumn()
                                    + " references non-primary-key column "
                                    + fk.getTargetRelation() + "." + fk.getTargetColumn()
                    );
                }
            }
        }
    }

    public List<String> topologicalOrder() {
        Map<String, Integer> indegree = new HashMap<>();
        for (String relation : adjacency.keySet()) {
            indegree.putIfAbsent(relation, 0);
        }
        for (Map.Entry<String, List<String>> entry : adjacency.entrySet()) {
            for (String child : entry.getValue()) {
                indegree.put(child, indegree.getOrDefault(child, 0) + 1);
            }
        }

        Queue<String> queue = new ArrayDeque<>();
        for (Map.Entry<String, Integer> entry : indegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.offer(entry.getKey());
            }
        }

        List<String> order = new ArrayList<>();
        while (!queue.isEmpty()) {
            String relation = queue.poll();
            order.add(relation);
            for (String child : adjacency.getOrDefault(relation, Collections.emptyList())) {
                int next = indegree.get(child) - 1;
                indegree.put(child, next);
                if (next == 0) {
                    queue.offer(child);
                }
            }
        }

        if (order.size() != adjacency.size()) {
            throw new IllegalStateException("Schema graph contains a cycle and is not a DAG.");
        }
        return order;
    }

    public void validateAcyclic() {
        validateForeignKeys();
        topologicalOrder();
    }

    public List<String> childrenOf(String relation) {
        return adjacency.getOrDefault(relation, Collections.emptyList());
    }

    public List<ForeignKey> foreignKeysFromChildToParent(String childRelation, String parentRelation) {
        RelationSchema child = relations.get(childRelation);
        if (child == null) {
            return Collections.emptyList();
        }

        List<ForeignKey> matches = new ArrayList<>();
        for (ForeignKey fk : child.getForeignKeys()) {
            if (fk.getTargetRelation().equals(parentRelation)) {
                matches.add(fk);
            }
        }
        return matches;
    }

    public Map<String, RelationSchema> getRelations() {
        return relations;
    }

    public Map<String, List<String>> getAdjacency() {
        return adjacency;
    }
}
