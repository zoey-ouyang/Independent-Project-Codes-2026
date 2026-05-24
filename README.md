# Reproducing Cquirrel on Apache Flink

This repository contains an IP2026 course project that reproduces the main ideas of the paper **Cquirrel: Continuous Query Processing over Acyclic Relational Schemas** on top of Apache Flink.

The project focuses on continuous query processing over acyclic primary-key / foreign-key schemas. It includes a correctness-oriented general maintenance prototype and a query-specific parallel Flink pipeline implementation for TPC-H Q3 and Q6.

## Main Features

- Validates acyclic primary-key / foreign-key schema graphs.
- Supports TPC-H-style FK-DAG join queries with `COUNT(*)` and `SUM(...)`.
- Maintains relation-local tuple state and foreign-key indexes.
- Recomputes live tuples and current answers after each update in the general prototype.
- Outputs positive and negative answer deltas by comparing old and new answer sets.
- Maintains grouped aggregate snapshots.
- Provides query-specific Flink pipelines for TPC-H Q3 and Q6.
- Records per-update latency, answer delta count, live tuple count, aggregate group count, memory usage, and correctness status.

## Project Structure

```text
.
├── README.md
├── IMPLEMENTATION_STATUS.md
├── pom.xml
├── data/
│   └── tpch-sf0.01/
├── queries/
│   ├── tpch/
│   └── tpch_fk/
├── results/
├── scripts/
├── src/main/java/org/example/cquirrel/
├── tools/
└── ip_report/
```

### Root Files

- `README.md`: GitHub project overview and run instructions.
- `IMPLEMENTATION_STATUS.md`: detailed mapping between the project requirements and implemented Java files.
- `pom.xml`: Maven configuration, dependencies, and build settings.

### Source Code

- `src/main/java/org/example/cquirrel/app/`
  - `EvaluationRunner.java`: runs the general FK-DAG maintenance evaluation and writes per-update metrics to CSV.
  - `MultiOperatorTpchJob.java`: runs query-specific Flink pipelines for TPC-H Q3 and Q6.
  - `TpchWorkloadFactory.java`: defines the TPC-H schema graph and supported query plans.

- `src/main/java/org/example/cquirrel/schema/`
  - Defines relation schemas, foreign keys, schema DAG validation, topological ordering, and DAG level planning.

- `src/main/java/org/example/cquirrel/query/`
  - Defines query plans, join edges, group-by columns, and aggregation specifications.

- `src/main/java/org/example/cquirrel/state/`
  - Contains the main in-memory maintenance logic.
  - Maintains tuple maps, foreign-key indexes, live tuple sets, answer deltas, and aggregate rows.

- `src/main/java/org/example/cquirrel/model/`
  - Defines tuple records, update events, update operations, and maintenance result objects.

- `src/main/java/org/example/cquirrel/result/`
  - Defines answer deltas, join answers, and aggregate output rows.

- `src/main/java/org/example/cquirrel/source/`
  - Reads TPC-H `.tbl` files and converts rows into update events.

### Query Files

- `queries/tpch_fk/`
  - Contains four representative FK-DAG queries derived from the TPC-H schema:
    - `q1_customer_revenue.sql`
    - `q2_order_revenue.sql`
    - `q3_nation_revenue.sql`
    - `q4_supplier_revenue.sql`

- `queries/tpch/`
  - Contains simplified query-specific workloads based on official TPC-H Q3 and Q6:
    - `q3.sql`
    - `q6.sql`

The four FK-DAG queries are not official numbered TPC-H queries. They are TPC-H-style queries derived from the TPC-H schema so that they match Cquirrel's acyclic primary-key / foreign-key setting. Q3 and Q6 are implemented as fixed Flink pipelines.

### Data

- `data/tpch-sf0.01/`
  - Contains sample TPC-H `.tbl` files at scale factor 0.01.
  - Required files for the current experiments:
    - `nation.tbl`
    - `supplier.tbl`
    - `customer.tbl`
    - `orders.tbl`
    - `lineitem.tbl`

### Scripts

- `scripts/run_evaluation.sh`
  - Builds the project and runs the general single-operator FK-DAG maintenance evaluation.

- `scripts/run_multi_operator.sh`
  - Builds the project and runs the query-specific Flink pipeline for Q3 or Q6.

### Results

- `results/`
  - Stores output CSV files from previous experiments.
  - Example files:
    - `evaluation_tpch_customer_revenue_sf001_sample.csv`
    - `evaluation_tpch_order_revenue_sf001_sample.csv`
    - `evaluation_tpch_nation_revenue_sf001_sample.csv`
    - `evaluation_tpch_supplier_revenue_sf001_sample.csv`
    - `multi_operator_q3_sf001_sample.csv`
    - `multi_operator_q6_sf001_sample.csv`

### Report

- `ip_report/`
  - Contains the LaTeX source and generated PDF for the course report.
  - `main.tex`: report source.
  - `main.pdf`: compiled report.
  - `fig/`: generated experiment figures.
  - `experiment_summary.tex`: auto-generated LaTeX table rows from result CSV files.

## Requirements

- Java 8 or later
- Maven
- Apache Flink dependencies are handled by Maven through `pom.xml`
- A TPC-H `.tbl` data directory

The repository includes a small sample under `data/tpch-sf0.01/`.

## Build

From the repository root:

```bash
mvn -DskipTests package
```

This creates the project jar under `target/`.

## Run the General FK-DAG Evaluation

The general evaluation runner supports four query names:

```text
customer_revenue
order_revenue
nation_revenue
supplier_revenue
```

Example:

```bash
QUERY=customer_revenue \
TPCH_DIR=data/tpch-sf0.01 \
TPCH_CUSTOMER_LIMIT=1500 \
TPCH_ORDERS_LIMIT=3000 \
TPCH_LINEITEM_LIMIT=1000 \
INCLUDE_DELETES=0 \
OUTPUT=results/evaluation_tpch_customer_revenue_sf001_sample.csv \
scripts/run_evaluation.sh
```

Run the other FK-DAG queries:

```bash
QUERY=order_revenue \
TPCH_DIR=data/tpch-sf0.01 \
TPCH_CUSTOMER_LIMIT=1500 \
TPCH_ORDERS_LIMIT=3000 \
TPCH_LINEITEM_LIMIT=1000 \
INCLUDE_DELETES=0 \
OUTPUT=results/evaluation_tpch_order_revenue_sf001_sample.csv \
scripts/run_evaluation.sh
```

```bash
QUERY=nation_revenue \
TPCH_DIR=data/tpch-sf0.01 \
TPCH_CUSTOMER_LIMIT=1500 \
TPCH_ORDERS_LIMIT=3000 \
TPCH_LINEITEM_LIMIT=1000 \
INCLUDE_DELETES=0 \
OUTPUT=results/evaluation_tpch_nation_revenue_sf001_sample.csv \
scripts/run_evaluation.sh
```

```bash
QUERY=supplier_revenue \
TPCH_DIR=data/tpch-sf0.01 \
TPCH_CUSTOMER_LIMIT=1500 \
TPCH_ORDERS_LIMIT=3000 \
TPCH_LINEITEM_LIMIT=1000 \
INCLUDE_DELETES=0 \
OUTPUT=results/evaluation_tpch_supplier_revenue_sf001_sample.csv \
scripts/run_evaluation.sh
```

The output CSV has the following columns:

```text
event_index,relation,operation,latency_micros,answer_delta_count,
live_tuple_count,aggregate_group_count,used_memory_bytes,correctness_status
```

`correctness_status` is set to `ok` at periodic full-recomputation checks.

## Run the Query-Specific Flink Pipelines

The multi-operator extension supports:

```text
Q3
Q6
```

Run Q3:

```bash
QUERY=Q3 \
TPCH_DIR=data/tpch-sf0.01 \
TPCH_CUSTOMER_LIMIT=1500 \
TPCH_ORDERS_LIMIT=3000 \
TPCH_LINEITEM_LIMIT=10000 \
PARALLELISM=2 \
OUTPUT=results/multi_operator_q3_sf001_sample.csv \
scripts/run_multi_operator.sh
```

Run Q6:

```bash
QUERY=Q6 \
TPCH_DIR=data/tpch-sf0.01 \
TPCH_LINEITEM_LIMIT=10000 \
PARALLELISM=2 \
OUTPUT=results/multi_operator_q6_sf001_sample.csv \
scripts/run_multi_operator.sh
```

The Q3 pipeline contains relation-specific filtering, keyed join maintenance, and revenue aggregation. The Q6 pipeline contains lineitem filtering and revenue aggregation.

## Notes on DAG Direction

The Cquirrel paper defines a foreign-key DAG edge from the relation containing the foreign key to the referenced primary-key relation. Internally, this implementation stores the schema graph in the reverse direction, from parent to child. This is an implementation choice for the correctness-oriented prototype: it makes live tuple recomputation simpler because a topological scan visits parent relations before child relations.

Both representations describe the same acyclic primary-key / foreign-key join structure.

## Limitations

- The general prototype does not include a full SQL parser.
- The general prototype recomputes live tuples and current answers after each update, so it is correctness-oriented rather than fully optimized.
- The Q3/Q6 multi-operator jobs are query-specific and are not generated automatically from arbitrary SQL.
- Full TPC-H Q1-Q22 benchmark execution is outside the current scope.

## Citation

The reproduced paper is:

```text
Q. Wang, C. Zhang, D. Alsayed, K. Yi, B. Wu, F. Li, and C. Zhan.
"Cquirrel: Continuous Query Processing over Acyclic Relational Schemas."
Proceedings of the VLDB Endowment, vol. 14, no. 12, pp. 2667-2670, 2021.
doi: 10.14778/3476311.3476315
```
