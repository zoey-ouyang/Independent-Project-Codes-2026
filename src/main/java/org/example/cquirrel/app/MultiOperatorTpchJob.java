package org.example.cquirrel.app;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.example.cquirrel.model.TupleData;
import org.example.cquirrel.model.UpdateEvent;
import org.example.cquirrel.model.UpdateOperation;
import org.example.cquirrel.source.TpchFileUpdateSource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public final class MultiOperatorTpchJob {

    private MultiOperatorTpchJob() {
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.fromArgs(args);
        List<UpdateEvent> updates = TpchFileUpdateSource.load(
                config.tpchDir,
                config.nationLimit,
                config.supplierLimit,
                config.customerLimit,
                config.ordersLimit,
                config.lineitemLimit,
                false
        );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.parallelism);

        DataStream<UpdateEvent> input = env.fromCollection(updates);
        DataStream<AggregateSnapshot> snapshots;
        if ("Q6".equalsIgnoreCase(config.query)) {
            snapshots = buildQ6(input);
        } else if ("Q3".equalsIgnoreCase(config.query)) {
            snapshots = buildQ3(input);
        } else {
            throw new IllegalArgumentException("Unsupported multi-operator query: " + config.query);
        }

        snapshots.addSink(new AggregateCsvSink(config.outputPath)).setParallelism(1);
        env.execute("Cquirrel multi-operator " + config.query);

        System.out.println("Multi-operator evaluation completed.");
        System.out.println("query=" + config.query);
        System.out.println("tpchDir=" + config.tpchDir);
        System.out.println("updates=" + updates.size());
        System.out.println("parallelism=" + config.parallelism);
        System.out.println("output=" + new File(config.outputPath).getAbsolutePath());
    }

    private static DataStream<AggregateSnapshot> buildQ3(DataStream<UpdateEvent> input) {
        DataStream<CustomerSignal> customerSignals = input
                .filter(new RelationFilter("customer"))
                .keyBy(new EventPrimaryKeySelector())
                .process(new Q3CustomerPredicateOperator())
                .name("Q3 customer predicate");

        DataStream<UpdateEvent> orders = input
                .filter(new RelationFilter("orders"))
                .name("Q3 orders updates");

        DataStream<OrderSignal> orderSignals = customerSignals
                .keyBy(new CustomerSignalKeySelector())
                .connect(orders.keyBy(new OrdersCustomerKeySelector()))
                .process(new Q3OrdersJoinOperator())
                .name("Q3 customer-orders join");

        DataStream<UpdateEvent> lineitems = input
                .filter(new RelationFilter("lineitem"))
                .name("Q3 lineitem updates");

        DataStream<RevenueDelta> deltas = orderSignals
                .keyBy(new OrderSignalKeySelector())
                .connect(lineitems.keyBy(new LineitemOrderKeySelector()))
                .process(new Q3LineitemJoinOperator())
                .name("Q3 orders-lineitem join");

        return deltas
                .keyBy(new RevenueDeltaGroupKeySelector())
                .process(new RevenueAggregationOperator())
                .name("Q3 revenue aggregation");
    }

    private static DataStream<AggregateSnapshot> buildQ6(DataStream<UpdateEvent> input) {
        DataStream<RevenueDelta> deltas = input
                .filter(new RelationFilter("lineitem"))
                .keyBy(new EventPrimaryKeySelector())
                .process(new Q6LineitemPredicateOperator())
                .name("Q6 lineitem predicate");

        return deltas
                .keyBy(new RevenueDeltaGroupKeySelector())
                .process(new RevenueAggregationOperator())
                .name("Q6 revenue aggregation");
    }

    private static final class RelationFilter implements FilterFunction<UpdateEvent> {
        private final String relation;

        private RelationFilter(String relation) {
            this.relation = relation;
        }

        @Override
        public boolean filter(UpdateEvent event) {
            return relation.equals(event.getRelation());
        }
    }

    private static final class Q3CustomerPredicateOperator
            extends KeyedProcessFunction<String, UpdateEvent, CustomerSignal> {

        @Override
        public void processElement(UpdateEvent event, Context context, Collector<CustomerSignal> out) {
            TupleData tuple = event.getPayload();
            if (tuple == null || !"AUTOMOBILE".equals(tuple.getAttributes().get("c_mktsegment"))) {
                return;
            }
            int delta = event.getOp() == UpdateOperation.DELETE ? -1 : 1;
            out.collect(new CustomerSignal(tuple.getPrimaryKey(), tuple, delta));
        }
    }

    private static final class Q3OrdersJoinOperator
            extends KeyedCoProcessFunction<String, CustomerSignal, UpdateEvent, OrderSignal> {

        private transient ValueState<TupleData> activeCustomer;
        private transient MapState<String, TupleData> storedOrders;

        @Override
        public void open(Configuration parameters) {
            activeCustomer = getRuntimeContext().getState(
                    new ValueStateDescriptor<TupleData>("active-customer", TupleData.class)
            );
            storedOrders = getRuntimeContext().getMapState(
                    new MapStateDescriptor<String, TupleData>("stored-orders", String.class, TupleData.class)
            );
        }

        @Override
        public void processElement1(CustomerSignal signal, Context context, Collector<OrderSignal> out)
                throws Exception {
            if (signal.delta > 0) {
                activeCustomer.update(signal.customer);
                for (TupleData order : storedOrders.values()) {
                    emitOrder(order, signal.customer, 1, out);
                }
            } else {
                TupleData customer = activeCustomer.value();
                if (customer != null) {
                    for (TupleData order : storedOrders.values()) {
                        emitOrder(order, customer, -1, out);
                    }
                }
                activeCustomer.clear();
            }
        }

        @Override
        public void processElement2(UpdateEvent event, Context context, Collector<OrderSignal> out)
                throws Exception {
            TupleData previous = storedOrders.get(event.getPrimaryKey());
            TupleData customer = activeCustomer.value();
            if (event.getOp() == UpdateOperation.DELETE) {
                if (previous != null && customer != null) {
                    emitOrder(previous, customer, -1, out);
                }
                storedOrders.remove(event.getPrimaryKey());
                return;
            }

            TupleData next = event.getPayload();
            if (previous != null && customer != null) {
                emitOrder(previous, customer, -1, out);
            }
            if (next != null && orderPassesQ3(next)) {
                storedOrders.put(next.getPrimaryKey(), next);
                if (customer != null) {
                    emitOrder(next, customer, 1, out);
                }
            } else {
                storedOrders.remove(event.getPrimaryKey());
            }
        }

        private void emitOrder(TupleData order, TupleData customer, int delta, Collector<OrderSignal> out) {
            Map<String, String> attrs = order.getAttributes();
            out.collect(new OrderSignal(
                    attrs.get("o_orderkey"),
                    attrs.get("o_orderdate"),
                    attrs.get("o_shippriority"),
                    customer.getPrimaryKey(),
                    delta
            ));
        }
    }

    private static final class Q3LineitemJoinOperator
            extends KeyedCoProcessFunction<String, OrderSignal, UpdateEvent, RevenueDelta> {

        private transient ValueState<OrderSignal> activeOrder;
        private transient MapState<String, TupleData> storedLineitems;

        @Override
        public void open(Configuration parameters) {
            activeOrder = getRuntimeContext().getState(
                    new ValueStateDescriptor<OrderSignal>("active-order", OrderSignal.class)
            );
            storedLineitems = getRuntimeContext().getMapState(
                    new MapStateDescriptor<String, TupleData>("stored-lineitems", String.class, TupleData.class)
            );
        }

        @Override
        public void processElement1(OrderSignal signal, Context context, Collector<RevenueDelta> out)
                throws Exception {
            if (signal.delta > 0) {
                activeOrder.update(signal);
                for (TupleData lineitem : storedLineitems.values()) {
                    emitRevenue(signal, lineitem, 1, out);
                }
            } else {
                OrderSignal order = activeOrder.value();
                if (order != null) {
                    for (TupleData lineitem : storedLineitems.values()) {
                        emitRevenue(order, lineitem, -1, out);
                    }
                }
                activeOrder.clear();
            }
        }

        @Override
        public void processElement2(UpdateEvent event, Context context, Collector<RevenueDelta> out)
                throws Exception {
            TupleData previous = storedLineitems.get(event.getPrimaryKey());
            OrderSignal order = activeOrder.value();
            if (event.getOp() == UpdateOperation.DELETE) {
                if (previous != null && order != null) {
                    emitRevenue(order, previous, -1, out);
                }
                storedLineitems.remove(event.getPrimaryKey());
                return;
            }

            TupleData next = event.getPayload();
            if (previous != null && order != null) {
                emitRevenue(order, previous, -1, out);
            }
            if (next != null && lineitemPassesQ3(next)) {
                storedLineitems.put(next.getPrimaryKey(), next);
                if (order != null) {
                    emitRevenue(order, next, 1, out);
                }
            } else {
                storedLineitems.remove(event.getPrimaryKey());
            }
        }

        private void emitRevenue(OrderSignal order, TupleData lineitem, int sign, Collector<RevenueDelta> out) {
            double revenue = extendedPrice(lineitem) * (1.0 - discount(lineitem)) * sign;
            String groupKey = order.orderKey + "|" + order.orderDate + "|" + order.shipPriority;
            out.collect(new RevenueDelta(groupKey, sign, revenue));
        }
    }

    private static final class Q6LineitemPredicateOperator
            extends KeyedProcessFunction<String, UpdateEvent, RevenueDelta> {

        private transient ValueState<TupleData> storedLineitem;

        @Override
        public void open(Configuration parameters) {
            storedLineitem = getRuntimeContext().getState(
                    new ValueStateDescriptor<TupleData>("q6-lineitem", TupleData.class)
            );
        }

        @Override
        public void processElement(UpdateEvent event, Context context, Collector<RevenueDelta> out)
                throws Exception {
            TupleData previous = storedLineitem.value();
            if (event.getOp() == UpdateOperation.DELETE) {
                if (previous != null && lineitemPassesQ6(previous)) {
                    out.collect(new RevenueDelta("__all__", -1, -q6Revenue(previous)));
                }
                storedLineitem.clear();
                return;
            }

            TupleData next = event.getPayload();
            if (previous != null && lineitemPassesQ6(previous)) {
                out.collect(new RevenueDelta("__all__", -1, -q6Revenue(previous)));
            }
            storedLineitem.update(next);
            if (next != null && lineitemPassesQ6(next)) {
                out.collect(new RevenueDelta("__all__", 1, q6Revenue(next)));
            }
        }
    }

    private static final class RevenueAggregationOperator
            extends KeyedProcessFunction<String, RevenueDelta, AggregateSnapshot> {

        private transient ValueState<Double> revenueState;
        private transient ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) {
            revenueState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("revenue", Double.class)
            );
            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("count", Long.class)
            );
        }

        @Override
        public void processElement(RevenueDelta delta, Context context, Collector<AggregateSnapshot> out)
                throws Exception {
            Double currentRevenue = revenueState.value();
            Long currentCount = countState.value();
            if (currentRevenue == null) {
                currentRevenue = 0.0;
            }
            if (currentCount == null) {
                currentCount = 0L;
            }
            double nextRevenue = currentRevenue + delta.revenueDelta;
            long nextCount = currentCount + delta.countDelta;
            revenueState.update(nextRevenue);
            countState.update(nextCount);
            out.collect(new AggregateSnapshot(delta.groupKey, nextCount, nextRevenue));
        }
    }

    private static final class AggregateCsvSink extends RichSinkFunction<AggregateSnapshot> {
        private final String outputPath;
        private transient BufferedWriter writer;

        private AggregateCsvSink(String outputPath) {
            this.outputPath = outputPath;
        }

        @Override
        public void open(Configuration parameters) throws IOException {
            File output = new File(outputPath);
            File parent = output.getParentFile();
            if (parent != null) {
                parent.mkdirs();
            }
            writer = new BufferedWriter(new FileWriter(output));
            writer.write("group_key,count,revenue\n");
        }

        @Override
        public void invoke(AggregateSnapshot value, Context context) throws IOException {
            writer.write(value.groupKey + "," + value.count + "," + value.revenue + "\n");
        }

        @Override
        public void close() throws IOException {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
        }
    }

    private static boolean orderPassesQ3(TupleData order) {
        String orderDate = order.getAttributes().get("o_orderdate");
        return orderDate != null && orderDate.compareTo("1995-03-13") < 0;
    }

    private static boolean lineitemPassesQ3(TupleData lineitem) {
        String shipDate = lineitem.getAttributes().get("l_shipdate");
        return shipDate != null && shipDate.compareTo("1995-03-13") > 0;
    }

    private static boolean lineitemPassesQ6(TupleData lineitem) {
        Map<String, String> attrs = lineitem.getAttributes();
        String shipDate = attrs.get("l_shipdate");
        double discount = parseDouble(attrs.get("l_discount"));
        double quantity = parseDouble(attrs.get("l_quantity"));
        return shipDate != null
                && shipDate.compareTo("1994-01-01") >= 0
                && shipDate.compareTo("1995-01-01") < 0
                && discount >= 0.05
                && discount <= 0.07
                && quantity < 24.0;
    }

    private static double q6Revenue(TupleData lineitem) {
        return extendedPrice(lineitem) * discount(lineitem);
    }

    private static double extendedPrice(TupleData lineitem) {
        return parseDouble(lineitem.getAttributes().get("l_extendedprice"));
    }

    private static double discount(TupleData lineitem) {
        return parseDouble(lineitem.getAttributes().get("l_discount"));
    }

    private static double parseDouble(String value) {
        if (value == null || value.trim().isEmpty()) {
            return 0.0;
        }
        return Double.parseDouble(value);
    }

    private static final class EventPrimaryKeySelector implements KeySelector<UpdateEvent, String> {
        @Override
        public String getKey(UpdateEvent event) {
            return event.getPrimaryKey();
        }
    }

    private static final class CustomerSignalKeySelector implements KeySelector<CustomerSignal, String> {
        @Override
        public String getKey(CustomerSignal signal) {
            return signal.customerKey;
        }
    }

    private static final class OrdersCustomerKeySelector implements KeySelector<UpdateEvent, String> {
        @Override
        public String getKey(UpdateEvent event) {
            TupleData tuple = event.getPayload();
            return tuple == null ? "" : tuple.getAttributes().get("o_custkey");
        }
    }

    private static final class OrderSignalKeySelector implements KeySelector<OrderSignal, String> {
        @Override
        public String getKey(OrderSignal signal) {
            return signal.orderKey;
        }
    }

    private static final class LineitemOrderKeySelector implements KeySelector<UpdateEvent, String> {
        @Override
        public String getKey(UpdateEvent event) {
            TupleData tuple = event.getPayload();
            return tuple == null ? "" : tuple.getAttributes().get("l_orderkey");
        }
    }

    private static final class RevenueDeltaGroupKeySelector implements KeySelector<RevenueDelta, String> {
        @Override
        public String getKey(RevenueDelta delta) {
            return delta.groupKey;
        }
    }

    public static final class CustomerSignal implements Serializable {
        private String customerKey;
        private TupleData customer;
        private int delta;

        public CustomerSignal() {
        }

        private CustomerSignal(String customerKey, TupleData customer, int delta) {
            this.customerKey = customerKey;
            this.customer = customer;
            this.delta = delta;
        }
    }

    public static final class OrderSignal implements Serializable {
        private String orderKey;
        private String orderDate;
        private String shipPriority;
        private String customerKey;
        private int delta;

        public OrderSignal() {
        }

        private OrderSignal(String orderKey, String orderDate, String shipPriority, String customerKey, int delta) {
            this.orderKey = orderKey;
            this.orderDate = orderDate;
            this.shipPriority = shipPriority;
            this.customerKey = customerKey;
            this.delta = delta;
        }
    }

    public static final class RevenueDelta implements Serializable {
        private String groupKey;
        private long countDelta;
        private double revenueDelta;

        public RevenueDelta() {
        }

        private RevenueDelta(String groupKey, long countDelta, double revenueDelta) {
            this.groupKey = groupKey;
            this.countDelta = countDelta;
            this.revenueDelta = revenueDelta;
        }
    }

    public static final class AggregateSnapshot implements Serializable {
        private String groupKey;
        private long count;
        private double revenue;

        public AggregateSnapshot() {
        }

        private AggregateSnapshot(String groupKey, long count, double revenue) {
            this.groupKey = groupKey;
            this.count = count;
            this.revenue = revenue;
        }
    }

    private static final class Config {
        private String query = "Q3";
        private String tpchDir = "data/tpch-sf0.01";
        private int nationLimit = 0;
        private int supplierLimit = 0;
        private int customerLimit = 1500;
        private int ordersLimit = 3000;
        private int lineitemLimit = 1000;
        private int parallelism = 2;
        private String outputPath = "results/multi_operator_q3.csv";

        private static Config fromArgs(String[] args) {
            Config config = new Config();
            for (int i = 0; i < args.length; i += 1) {
                String arg = args[i];
                if ("--query".equals(arg)) {
                    config.query = args[++i].toUpperCase();
                } else if ("--tpch-dir".equals(arg)) {
                    config.tpchDir = args[++i];
                } else if ("--nation-limit".equals(arg)) {
                    config.nationLimit = Integer.parseInt(args[++i]);
                } else if ("--supplier-limit".equals(arg)) {
                    config.supplierLimit = Integer.parseInt(args[++i]);
                } else if ("--customer-limit".equals(arg)) {
                    config.customerLimit = Integer.parseInt(args[++i]);
                } else if ("--orders-limit".equals(arg)) {
                    config.ordersLimit = Integer.parseInt(args[++i]);
                } else if ("--lineitem-limit".equals(arg)) {
                    config.lineitemLimit = Integer.parseInt(args[++i]);
                } else if ("--parallelism".equals(arg)) {
                    config.parallelism = Integer.parseInt(args[++i]);
                } else if ("--output".equals(arg)) {
                    config.outputPath = args[++i];
                } else {
                    throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }
            if ("Q6".equals(config.query) && "results/multi_operator_q3.csv".equals(config.outputPath)) {
                config.outputPath = "results/multi_operator_q6.csv";
            }
            return config;
        }
    }
}
