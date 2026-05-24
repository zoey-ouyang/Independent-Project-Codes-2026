# Evaluation Results

This directory stores CSV outputs produced by `org.example.cquirrel.app.EvaluationRunner`.

Example:

```bash
TPCH_DIR=data/tpch-sf0.01 \
TPCH_CUSTOMER_LIMIT=1500 \
TPCH_ORDERS_LIMIT=3000 \
TPCH_LINEITEM_LIMIT=1000 \
INCLUDE_DELETES=0 \
OUTPUT=results/evaluation_tpch_customer_revenue_sf001_sample.csv \
scripts/run_evaluation.sh
```

The generated CSV contains one row per update event:

- `event_index`
- `relation`
- `operation`
- `latency_micros`
- `answer_delta_count`
- `live_tuple_count`
- `aggregate_group_count`
- `used_memory_bytes`
- `correctness_status`

Rows with `correctness_status=ok` mark periodic full-recomputation checks.

To run with another TPC-H `.tbl` data directory:

```bash
TPCH_DIR=/path/to/tpch-dbgen-data \
TPCH_LINEITEM_LIMIT=1000 \
TPCH_ORDERS_LIMIT=1000 \
TPCH_CUSTOMER_LIMIT=1000 \
INCLUDE_DELETES=0 \
OUTPUT=results/evaluation_tpch_sample.csv \
scripts/run_evaluation.sh
```
