package org.example.cquirrel.schema;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class DagPlanner {

    private DagPlanner() {
    }

    public static DagPlan buildLevelPlan(SchemaGraph schemaGraph) {
        List<String> order = schemaGraph.topologicalOrder();
        Map<String, Integer> levels = new LinkedHashMap<>();

        for (String relation : order) {
            int level = 0;
            RelationSchema schema = schemaGraph.getRelations().get(relation);
            for (ForeignKey foreignKey : schema.getForeignKeys()) {
                Integer parentLevel = levels.get(foreignKey.getTargetRelation());
                if (parentLevel == null) {
                    throw new IllegalStateException(
                            "Parent relation was not planned before child relation: " + foreignKey
                    );
                }
                level = Math.max(level, parentLevel + 1);
            }
            levels.put(relation, level);
        }

        return new DagPlan(order, levels);
    }
}
