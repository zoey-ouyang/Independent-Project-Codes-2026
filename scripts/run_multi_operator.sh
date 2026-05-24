#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

mvn -q -DskipTests package

QUERY_NAME="${QUERY:-Q3}"
DEFAULT_OUTPUT="results/multi_operator_$(echo "$QUERY_NAME" | tr '[:upper:]' '[:lower:]')_sf001_sample.csv"

java -cp target/classes:target/cquirrel-flink-0.1.0-SNAPSHOT.jar \
  org.example.cquirrel.app.MultiOperatorTpchJob \
  --query "$QUERY_NAME" \
  --tpch-dir "${TPCH_DIR:-data/tpch-sf0.01}" \
  --customer-limit "${TPCH_CUSTOMER_LIMIT:-1500}" \
  --orders-limit "${TPCH_ORDERS_LIMIT:-3000}" \
  --lineitem-limit "${TPCH_LINEITEM_LIMIT:-10000}" \
  --parallelism "${PARALLELISM:-2}" \
  --output "${OUTPUT:-$DEFAULT_OUTPUT}"
