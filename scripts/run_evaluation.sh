#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

mvn -q -DskipTests package

ARGS=(
  --query "${QUERY:-customer_revenue}"
  --tpch-dir "${TPCH_DIR:-data/tpch-sf0.01}"
  --correctness-every "${CORRECTNESS_EVERY:-100}"
  --output "${OUTPUT:-results/evaluation.csv}"
)

if [[ -n "${TPCH_ROW_LIMIT:-}" ]]; then
  ARGS+=(--tpch-row-limit "$TPCH_ROW_LIMIT")
fi

if [[ -n "${TPCH_NATION_LIMIT:-}" ]]; then
  ARGS+=(--tpch-nation-limit "$TPCH_NATION_LIMIT")
fi

if [[ -n "${TPCH_SUPPLIER_LIMIT:-}" ]]; then
  ARGS+=(--tpch-supplier-limit "$TPCH_SUPPLIER_LIMIT")
fi

if [[ -n "${TPCH_CUSTOMER_LIMIT:-}" ]]; then
  ARGS+=(--tpch-customer-limit "$TPCH_CUSTOMER_LIMIT")
fi

if [[ -n "${TPCH_ORDERS_LIMIT:-}" ]]; then
  ARGS+=(--tpch-orders-limit "$TPCH_ORDERS_LIMIT")
fi

if [[ -n "${TPCH_LINEITEM_LIMIT:-}" ]]; then
  ARGS+=(--tpch-lineitem-limit "$TPCH_LINEITEM_LIMIT")
fi

if [[ "${INCLUDE_DELETES:-1}" == "0" ]]; then
  ARGS+=(--no-deletes)
fi

java -cp target/classes:target/cquirrel-flink-0.1.0-SNAPSHOT.jar \
  org.example.cquirrel.app.EvaluationRunner \
  "${ARGS[@]}"
