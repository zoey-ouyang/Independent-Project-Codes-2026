package org.example.cquirrel.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DagPlan implements Serializable {

    private final List<String> topologicalOrder;
    private final Map<String, Integer> relationLevels;
    private final List<List<String>> levels;

    public DagPlan(List<String> topologicalOrder, Map<String, Integer> relationLevels) {
        this.topologicalOrder = new ArrayList<>(topologicalOrder);
        this.relationLevels = new LinkedHashMap<>(relationLevels);
        this.levels = buildLevels(topologicalOrder, relationLevels);
    }

    private static List<List<String>> buildLevels(List<String> order, Map<String, Integer> relationLevels) {
        List<List<String>> result = new ArrayList<>();
        for (String relation : order) {
            int level = relationLevels.get(relation);
            while (result.size() <= level) {
                result.add(new ArrayList<String>());
            }
            result.get(level).add(relation);
        }
        return result;
    }

    public List<String> getTopologicalOrder() {
        return Collections.unmodifiableList(topologicalOrder);
    }

    public Map<String, Integer> getRelationLevels() {
        return Collections.unmodifiableMap(relationLevels);
    }

    public List<List<String>> getLevels() {
        List<List<String>> copy = new ArrayList<>();
        for (List<String> level : levels) {
            copy.add(Collections.unmodifiableList(level));
        }
        return Collections.unmodifiableList(copy);
    }

    @Override
    public String toString() {
        return "DagPlan{"
                + "topologicalOrder=" + topologicalOrder
                + ", relationLevels=" + relationLevels
                + ", levels=" + levels
                + '}';
    }
}
