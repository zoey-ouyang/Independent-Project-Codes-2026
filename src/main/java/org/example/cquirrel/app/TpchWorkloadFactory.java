package org.example.cquirrel.app;

import org.example.cquirrel.query.JoinEdge;
import org.example.cquirrel.query.QueryPlan;
import org.example.cquirrel.schema.ForeignKey;
import org.example.cquirrel.schema.RelationSchema;
import org.example.cquirrel.schema.SchemaGraph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class TpchWorkloadFactory {

    public static final String CUSTOMER_REVENUE = "customer_revenue";
    public static final String ORDER_REVENUE = "order_revenue";
    public static final String NATION_REVENUE = "nation_revenue";
    public static final String SUPPLIER_REVENUE = "supplier_revenue";

    private TpchWorkloadFactory() {
    }

    public static SchemaGraph buildSchemaGraph() {
        RelationSchema nation = new RelationSchema(
                "nation",
                Collections.singletonList("n_nationkey"),
                Collections.<ForeignKey>emptyList()
        );

        RelationSchema customer = new RelationSchema(
                "customer",
                Collections.singletonList("c_custkey"),
                Collections.singletonList(new ForeignKey("c_nationkey", "nation", "n_nationkey"))
        );

        RelationSchema orders = new RelationSchema(
                "orders",
                Collections.singletonList("o_orderkey"),
                Collections.singletonList(new ForeignKey("o_custkey", "customer", "c_custkey"))
        );

        RelationSchema supplier = new RelationSchema(
                "supplier",
                Collections.singletonList("s_suppkey"),
                Collections.singletonList(new ForeignKey("s_nationkey", "nation", "n_nationkey"))
        );

        RelationSchema lineitem = new RelationSchema(
                "lineitem",
                Arrays.asList("l_orderkey", "l_linenumber"),
                Arrays.asList(
                        new ForeignKey("l_orderkey", "orders", "o_orderkey"),
                        new ForeignKey("l_suppkey", "supplier", "s_suppkey")
                )
        );

        List<RelationSchema> schemas = new ArrayList<>();
        schemas.add(nation);
        schemas.add(customer);
        schemas.add(orders);
        schemas.add(supplier);
        schemas.add(lineitem);
        return SchemaGraph.fromSchemas(schemas);
    }

    public static QueryPlan buildQueryPlan(String queryName) {
        if (CUSTOMER_REVENUE.equals(queryName)) {
            return customerRevenue();
        }
        if (ORDER_REVENUE.equals(queryName)) {
            return orderRevenue();
        }
        if (NATION_REVENUE.equals(queryName)) {
            return nationRevenue();
        }
        if (SUPPLIER_REVENUE.equals(queryName)) {
            return supplierRevenue();
        }
        throw new IllegalArgumentException(
                "Unknown query: " + queryName
                        + ". Supported queries: " + CUSTOMER_REVENUE
                        + ", " + ORDER_REVENUE
                        + ", " + NATION_REVENUE
                        + ", " + SUPPLIER_REVENUE
        );
    }

    private static QueryPlan customerRevenue() {
        QueryPlan plan = new QueryPlan(CUSTOMER_REVENUE);
        plan.addRelation("customer");
        plan.addRelation("orders");
        plan.addRelation("lineitem");
        plan.addJoinEdge(new JoinEdge("customer", "c_custkey", "orders", "o_custkey"));
        plan.addJoinEdge(new JoinEdge("orders", "o_orderkey", "lineitem", "l_orderkey"));
        plan.addProjection("customer.c_custkey");
        plan.addGroupBy("customer.c_custkey");
        plan.addAggregation("COUNT(*)");
        plan.addAggregation("SUM(lineitem.l_extendedprice)");
        return plan;
    }

    private static QueryPlan orderRevenue() {
        QueryPlan plan = new QueryPlan(ORDER_REVENUE);
        plan.addRelation("orders");
        plan.addRelation("lineitem");
        plan.addJoinEdge(new JoinEdge("orders", "o_orderkey", "lineitem", "l_orderkey"));
        plan.addProjection("orders.o_orderkey");
        plan.addGroupBy("orders.o_orderkey");
        plan.addAggregation("COUNT(*)");
        plan.addAggregation("SUM(lineitem.l_extendedprice)");
        return plan;
    }

    private static QueryPlan nationRevenue() {
        QueryPlan plan = new QueryPlan(NATION_REVENUE);
        plan.addRelation("nation");
        plan.addRelation("customer");
        plan.addRelation("orders");
        plan.addRelation("lineitem");
        plan.addJoinEdge(new JoinEdge("nation", "n_nationkey", "customer", "c_nationkey"));
        plan.addJoinEdge(new JoinEdge("customer", "c_custkey", "orders", "o_custkey"));
        plan.addJoinEdge(new JoinEdge("orders", "o_orderkey", "lineitem", "l_orderkey"));
        plan.addProjection("nation.n_nationkey");
        plan.addGroupBy("nation.n_nationkey");
        plan.addAggregation("COUNT(*)");
        plan.addAggregation("SUM(lineitem.l_extendedprice)");
        return plan;
    }

    private static QueryPlan supplierRevenue() {
        QueryPlan plan = new QueryPlan(SUPPLIER_REVENUE);
        plan.addRelation("supplier");
        plan.addRelation("lineitem");
        plan.addJoinEdge(new JoinEdge("supplier", "s_suppkey", "lineitem", "l_suppkey"));
        plan.addProjection("supplier.s_suppkey");
        plan.addGroupBy("supplier.s_suppkey");
        plan.addAggregation("COUNT(*)");
        plan.addAggregation("SUM(lineitem.l_extendedprice)");
        return plan;
    }
}
