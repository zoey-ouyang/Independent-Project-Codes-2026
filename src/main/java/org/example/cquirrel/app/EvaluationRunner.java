package org.example.cquirrel.app;

import org.example.cquirrel.model.RelationUpdateResult;
import org.example.cquirrel.model.TupleData;
import org.example.cquirrel.model.UpdateEvent;
import org.example.cquirrel.model.UpdateOperation;
import org.example.cquirrel.query.QueryPlan;
import org.example.cquirrel.result.AggregateRow;
import org.example.cquirrel.schema.DagPlan;
import org.example.cquirrel.schema.DagPlanner;
import org.example.cquirrel.schema.SchemaGraph;
import org.example.cquirrel.source.TpchFileUpdateSource;
import org.example.cquirrel.state.RelationStateStore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class EvaluationRunner {

    private EvaluationRunner() {
    }

    public static void main(String[] args) throws IOException {
        Config config = Config.fromArgs(args);
        SchemaGraph schemaGraph = TpchWorkloadFactory.buildSchemaGraph();
        QueryPlan queryPlan = TpchWorkloadFactory.buildQueryPlan(config.queryName);
        DagPlan dagPlan = DagPlanner.buildLevelPlan(schemaGraph);

        List<UpdateEvent> updates = TpchFileUpdateSource.load(
                config.tpchDir,
                config.tpchNationLimit,
                config.tpchSupplierLimit,
                config.tpchCustomerLimit,
                config.tpchOrdersLimit,
                config.tpchLineitemLimit,
                config.includeDeletes
        );

        RelationStateStore store = new RelationStateStore(schemaGraph, queryPlan);
        ExpectedState expectedState = new ExpectedState();

        File output = new File(config.outputPath);
        File parent = output.getParentFile();
        if (parent != null) {
            parent.mkdirs();
        }

        long startNanos = System.nanoTime();
        long lastNanos = startNanos;
        int correctnessChecks = 0;

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(output))) {
            writer.write("event_index,relation,operation,latency_micros,answer_delta_count,live_tuple_count,aggregate_group_count,used_memory_bytes,correctness_status\n");
            int eventIndex = 0;
            for (UpdateEvent update : updates) {
                eventIndex += 1;
                long before = System.nanoTime();
                RelationUpdateResult result = store.apply(update);
                long after = System.nanoTime();

                expectedState.apply(update);
                String correctnessStatus = "";
                if (eventIndex % config.correctnessEvery == 0 || eventIndex == updates.size()) {
                    compareAggregates(expectedState.expectedAggregates(config.queryName), result.getAggregateRows());
                    correctnessChecks += 1;
                    correctnessStatus = "ok";
                }

                writer.write(eventIndex + ","
                        + update.getRelation() + ","
                        + update.getOp() + ","
                        + ((after - before) / 1000L) + ","
                        + result.getAnswerDeltas().size() + ","
                        + result.getLiveTupleCount() + ","
                        + result.getAggregateRows().size() + ","
                        + usedMemoryBytes() + ","
                        + correctnessStatus
                        + "\n");
                lastNanos = after;
            }
        }

        long totalMillis = (lastNanos - startNanos) / 1_000_000L;
        double throughput = updates.isEmpty() ? 0.0 : (updates.size() * 1000.0) / Math.max(1L, totalMillis);

        System.out.println("Evaluation completed.");
        System.out.println("query=" + config.queryName);
        System.out.println("source=tpch-files");
        System.out.println("tpchDir=" + config.tpchDir);
        System.out.println("DAG plan: " + dagPlan);
        System.out.println("updates=" + updates.size());
        System.out.println("totalMillis=" + totalMillis);
        System.out.println("throughputEventsPerSec=" + throughput);
        System.out.println("correctnessChecks=" + correctnessChecks);
        System.out.println("output=" + output.getAbsolutePath());
    }

    private static long usedMemoryBytes() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static void compareAggregates(
            Map<String, AggregateRow> expected,
            Map<String, AggregateRow> actual
    ) {
        if (!expected.keySet().equals(actual.keySet())) {
            throw new IllegalStateException("Aggregate group mismatch. expected=" + expected + ", actual=" + actual);
        }
        for (Map.Entry<String, AggregateRow> entry : expected.entrySet()) {
            String groupKey = entry.getKey();
            AggregateRow expectedRow = entry.getValue();
            AggregateRow actualRow = actual.get(groupKey);
            if (expectedRow.getCount() != actualRow.getCount()) {
                throw new IllegalStateException(
                        "Aggregate count mismatch for group " + groupKey
                                + ". expected=" + expectedRow
                                + ", actual=" + actualRow
                );
            }
            if (!expectedRow.getSums().keySet().equals(actualRow.getSums().keySet())) {
                throw new IllegalStateException(
                        "Aggregate SUM expression mismatch for group " + groupKey
                                + ". expected=" + expectedRow
                                + ", actual=" + actualRow
                );
            }
            for (Map.Entry<String, Double> sumEntry : expectedRow.getSums().entrySet()) {
                String expression = sumEntry.getKey();
                double expectedSum = sumEntry.getValue();
                double actualSum = actualRow.getSums().get(expression);
                double tolerance = Math.max(0.0001, Math.abs(expectedSum) * 0.000000001);
                if (Math.abs(expectedSum - actualSum) > tolerance) {
                    throw new IllegalStateException(
                            "Aggregate SUM mismatch for group " + groupKey
                                    + ", expression=" + expression
                                    + ". expected=" + expectedRow
                                    + ", actual=" + actualRow
                    );
                }
            }
        }
    }

    private static final class ExpectedState {
        private final Map<String, TupleData> nations = new LinkedHashMap<>();
        private final Map<String, TupleData> suppliers = new LinkedHashMap<>();
        private final Map<String, TupleData> customers = new LinkedHashMap<>();
        private final Map<String, TupleData> orders = new LinkedHashMap<>();
        private final Map<String, TupleData> lineitems = new LinkedHashMap<>();

        private void apply(UpdateEvent event) {
            Map<String, TupleData> target = relationMap(event.getRelation());
            if (event.getOp() == UpdateOperation.DELETE) {
                target.remove(event.getPrimaryKey());
            } else {
                target.put(event.getPrimaryKey(), event.getPayload());
            }
        }

        private Map<String, TupleData> relationMap(String relation) {
            if ("nation".equals(relation)) {
                return nations;
            }
            if ("supplier".equals(relation)) {
                return suppliers;
            }
            if ("customer".equals(relation)) {
                return customers;
            }
            if ("orders".equals(relation)) {
                return orders;
            }
            if ("lineitem".equals(relation)) {
                return lineitems;
            }
            throw new IllegalArgumentException("Unknown relation: " + relation);
        }

        private Map<String, AggregateRow> expectedAggregates(String queryName) {
            Map<String, AggregateRow> aggregates = new LinkedHashMap<>();
            for (TupleData lineitem : lineitems.values()) {
                TupleData order = orders.get(lineitem.getAttributes().get("l_orderkey"));
                if (order == null) {
                    continue;
                }
                String customerKey = order.getAttributes().get("o_custkey");
                TupleData customer = customers.get(customerKey);
                if (customer == null) {
                    continue;
                }
                String supplierKey = lineitem.getAttributes().get("l_suppkey");
                TupleData supplier = suppliers.get(supplierKey);
                if (supplier == null) {
                    continue;
                }

                String groupKey = groupKey(queryName, order, customer, supplier);
                if (groupKey == null) {
                    continue;
                }
                AggregateRow row = aggregates.computeIfAbsent(groupKey, ignored -> new AggregateRow());
                row.incrementCount();
                row.addSum(
                        "SUM(lineitem.l_extendedprice)",
                        Double.parseDouble(lineitem.getAttributes().get("l_extendedprice"))
                );
            }
            return aggregates;
        }

        private String groupKey(
                String queryName,
                TupleData order,
                TupleData customer,
                TupleData supplier
        ) {
            if (TpchWorkloadFactory.CUSTOMER_REVENUE.equals(queryName)) {
                return customer.getAttributes().get("c_custkey");
            }
            if (TpchWorkloadFactory.ORDER_REVENUE.equals(queryName)) {
                return order.getAttributes().get("o_orderkey");
            }
            if (TpchWorkloadFactory.NATION_REVENUE.equals(queryName)) {
                String nationKey = customer.getAttributes().get("c_nationkey");
                return nations.containsKey(nationKey) ? nationKey : null;
            }
            if (TpchWorkloadFactory.SUPPLIER_REVENUE.equals(queryName)) {
                return supplier.getAttributes().get("s_suppkey");
            }
            throw new IllegalArgumentException("Unknown query: " + queryName);
        }
    }

    private static final class Config {
        private int correctnessEvery = 100;
        private boolean includeDeletes = true;
        private String queryName = TpchWorkloadFactory.CUSTOMER_REVENUE;
        private String outputPath = "results/evaluation.csv";
        private String tpchDir = "data/tpch-sf0.01";
        private int tpchNationLimit = 0;
        private int tpchSupplierLimit = 0;
        private int tpchCustomerLimit = 0;
        private int tpchOrdersLimit = 0;
        private int tpchLineitemLimit = 0;

        private static Config fromArgs(String[] args) {
            Config config = new Config();
            for (int i = 0; i < args.length; i += 1) {
                String arg = args[i];
                if ("--correctness-every".equals(arg)) {
                    config.correctnessEvery = Integer.parseInt(args[++i]);
                } else if ("--no-deletes".equals(arg)) {
                    config.includeDeletes = false;
                } else if ("--query".equals(arg)) {
                    config.queryName = args[++i];
                } else if ("--output".equals(arg)) {
                    config.outputPath = args[++i];
                } else if ("--tpch-dir".equals(arg)) {
                    config.tpchDir = args[++i];
                } else if ("--tpch-row-limit".equals(arg)) {
                    int limit = Integer.parseInt(args[++i]);
                    config.tpchNationLimit = limit;
                    config.tpchSupplierLimit = limit;
                    config.tpchCustomerLimit = limit;
                    config.tpchOrdersLimit = limit;
                    config.tpchLineitemLimit = limit;
                } else if ("--tpch-nation-limit".equals(arg)) {
                    config.tpchNationLimit = Integer.parseInt(args[++i]);
                } else if ("--tpch-supplier-limit".equals(arg)) {
                    config.tpchSupplierLimit = Integer.parseInt(args[++i]);
                } else if ("--tpch-customer-limit".equals(arg)) {
                    config.tpchCustomerLimit = Integer.parseInt(args[++i]);
                } else if ("--tpch-orders-limit".equals(arg)) {
                    config.tpchOrdersLimit = Integer.parseInt(args[++i]);
                } else if ("--tpch-lineitem-limit".equals(arg)) {
                    config.tpchLineitemLimit = Integer.parseInt(args[++i]);
                } else {
                    throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }
            return config;
        }
    }
}
