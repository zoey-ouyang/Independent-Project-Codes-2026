# IP2026 Implementation Status

This document maps the implementation plan to concrete files in this repository.

## 3.1 Reproducing Core Components

### Build/validate acyclic FK DAG schema and supported SPJA query constraints

Implemented in:

- `src/main/java/org/example/cquirrel/schema/SchemaGraph.java`
- `src/main/java/org/example/cquirrel/schema/RelationSchema.java`
- `src/main/java/org/example/cquirrel/schema/ForeignKey.java`
- `src/main/java/org/example/cquirrel/query/QueryPlan.java`
- `src/main/java/org/example/cquirrel/query/JoinEdge.java`

The schema graph validates:

- all FK targets exist
- FK target columns are primary-key columns
- the FK graph is acyclic
- query join edges are backed by schema PK-FK edges
- aggregations are limited to `COUNT(*)` and `SUM(relation.column)`

### Construct the foreign-key DAG plan and process updates level-by-level

Implemented in:

- `src/main/java/org/example/cquirrel/schema/DagPlanner.java`
- `src/main/java/org/example/cquirrel/schema/DagPlan.java`
- `src/main/java/org/example/cquirrel/state/RelationStateStore.java`

The DAG planner builds:

- topological relation order
- relation level numbers
- level groups for parent-before-child processing

### Implement relation keyed state / index for efficient insert/delete and key-based lookup

Implemented in:

- `src/main/java/org/example/cquirrel/state/RelationStateStore.java`
- `src/main/java/org/example/cquirrel/app/MultiOperatorTpchJob.java`

State maintained per relation:

- `tuplesByRelation`: primary-key to tuple
- `fkIndexByRelation`: FK value to child primary keys
- `liveKeysByRelation`: currently live tuple keys
- Flink keyed state in the query-specific Q3/Q6 multi-operator extension

### Maintain alive vs. non-live tuples and propagate state changes bottom-up along the DAG

Implemented in:

- `RelationStateStore.recomputeLiveTuples()`
- `RelationStateStore.allParentsAreLive(...)`

A tuple is live if all FK parents required by the DAG are live. Root tuples are live when present.

### Output delta enumeration for query answers, and incrementally maintain aggregation

Implemented in:

- `RelationStateStore.enumerateCurrentAnswers()`
- `RelationStateStore.diffAnswers(...)`
- `RelationStateStore.aggregateByGroup(...)`
- `src/main/java/org/example/cquirrel/result/AnswerDelta.java`
- `src/main/java/org/example/cquirrel/result/AggregateRow.java`

Supported outputs:

- positive answer deltas for newly produced join answers
- negative answer deltas for removed join answers
- grouped `COUNT(*)`
- grouped `SUM(relation.column)`

## 3.2 Integration with Flink

Implemented in:

- `src/main/java/org/example/cquirrel/app/EvaluationRunner.java`
- `src/main/java/org/example/cquirrel/app/MultiOperatorTpchJob.java`

The runnable Flink job:

- reads TPC-H `.tbl` rows as update events
- runs the fixed Q3/Q6 multi-operator pipelines with Flink keyed state
- emits aggregate snapshots to CSV

Current limitation:

- the general QueryPlan path uses a single logical maintenance operator for correctness and reproducibility
- `MultiOperatorTpchJob` adds query-specific multi-operator pipelines for TPC-H Q3 and Q6, but it is not a general SQL-to-operator compiler
- the project does not yet split arbitrary relations/levels into separate distributed operators exactly as in a full production Cquirrel implementation

### Query-specific multi-operator Q3/Q6 extension

Implemented in:

- `src/main/java/org/example/cquirrel/app/MultiOperatorTpchJob.java`
- `queries/tpch/q3.sql`
- `queries/tpch/q6.sql`
- `scripts/run_multi_operator.sh`

The Q3 pipeline is decomposed into:

- customer predicate operator
- customer-orders keyed join operator
- orders-lineitem keyed join operator
- revenue aggregation operator
- CSV sink

The Q6 pipeline is decomposed into:

- lineitem predicate operator
- revenue aggregation operator
- CSV sink

## 3.3 Evaluation

Implemented in:

- `src/main/java/org/example/cquirrel/app/EvaluationRunner.java`
- `src/main/java/org/example/cquirrel/source/TpchFileUpdateSource.java`
- `scripts/run_evaluation.sh`
- `scripts/run_multi_operator.sh`
- `results/README.md`

Evaluation support:

- representative TPC-H FK-DAG queries in `queries/tpch_fk`
- `customer -> orders -> lineitem`, grouped by customer
- `orders -> lineitem`, grouped by order
- `nation -> customer -> orders -> lineitem`, grouped by nation
- `supplier -> lineitem`, grouped by supplier
- configurable TPC-H row limits
- TPC-H `.tbl` file ingestion for `nation`, `supplier`, `customer`, `orders`, and `lineitem`
- query-specific multi-operator outputs for TPC-H Q3 and Q6
- insert and delete updates
- per-update latency CSV
- answer delta count
- live tuple count
- aggregate group count
- JVM memory proxy
- periodic correctness checks by full recomputation

## Verification Commands

```bash
mvn -q -DskipTests package
java -cp target/classes org.example.cquirrel.app.EvaluationRunner \
  --query customer_revenue \
  --tpch-dir data/tpch-sf0.01 \
  --tpch-customer-limit 1500 \
  --tpch-orders-limit 3000 \
  --tpch-lineitem-limit 1000 \
  --no-deletes \
  --output results/evaluation_tpch_customer_revenue_sf001_sample.csv
QUERY=Q6 TPCH_DIR=data/tpch-sf0.01 TPCH_LINEITEM_LIMIT=1000 \
  OUTPUT=results/multi_operator_q6_sf001_sample.csv \
  scripts/run_multi_operator.sh
```

All commands above are expected to pass.

