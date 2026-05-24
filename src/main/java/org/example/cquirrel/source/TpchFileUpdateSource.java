package org.example.cquirrel.source;

import org.example.cquirrel.model.TupleData;
import org.example.cquirrel.model.UpdateEvent;
import org.example.cquirrel.model.UpdateOperation;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TpchFileUpdateSource {

    private TpchFileUpdateSource() {
    }

    public static List<UpdateEvent> load(
            String tpchDirectory,
            int nationLimit,
            int supplierLimit,
            int customerLimit,
            int ordersLimit,
            int lineitemLimit,
            boolean includeDeletes
    ) throws IOException {
        Path root = Paths.get(tpchDirectory);
        List<UpdateEvent> updates = new ArrayList<>();
        long[] timestamp = new long[] {1L};

        readNation(root.resolve("nation.tbl"), nationLimit, updates, timestamp);
        readSupplier(root.resolve("supplier.tbl"), supplierLimit, updates, timestamp);
        readCustomer(root.resolve("customer.tbl"), customerLimit, updates, timestamp);
        readOrders(root.resolve("orders.tbl"), ordersLimit, updates, timestamp);
        String firstLineitemKey = readLineitem(root.resolve("lineitem.tbl"), lineitemLimit, updates, timestamp);

        if (includeDeletes && firstLineitemKey != null) {
            updates.add(new UpdateEvent(
                    UpdateOperation.DELETE,
                    "lineitem",
                    firstLineitemKey,
                    null,
                    timestamp[0]++
            ));
        }

        return updates;
    }

    private static void readNation(
            Path path,
            int limit,
            List<UpdateEvent> updates,
            long[] timestamp
    ) throws IOException {
        readRows(path, limit, parts -> {
            requireColumns(path, parts, 2);
            String key = parts[0];
            updates.add(insert("nation", key, timestamp, attributes(
                    "n_nationkey", parts[0],
                    "n_name", parts[1],
                    "n_regionkey", value(parts, 2),
                    "n_comment", value(parts, 3)
            )));
        });
    }

    private static void readSupplier(
            Path path,
            int limit,
            List<UpdateEvent> updates,
            long[] timestamp
    ) throws IOException {
        readRows(path, limit, parts -> {
            requireColumns(path, parts, 4);
            String key = parts[0];
            updates.add(insert("supplier", key, timestamp, attributes(
                    "s_suppkey", parts[0],
                    "s_name", parts[1],
                    "s_address", value(parts, 2),
                    "s_nationkey", parts[3],
                    "s_phone", value(parts, 4),
                    "s_acctbal", value(parts, 5),
                    "s_comment", value(parts, 6)
            )));
        });
    }

    private static void readCustomer(
            Path path,
            int limit,
            List<UpdateEvent> updates,
            long[] timestamp
    ) throws IOException {
        readRows(path, limit, parts -> {
            requireColumns(path, parts, 4);
            String key = parts[0];
            updates.add(insert("customer", key, timestamp, attributes(
                    "c_custkey", parts[0],
                    "c_name", parts[1],
                    "c_address", value(parts, 2),
                    "c_nationkey", parts[3],
                    "c_phone", value(parts, 4),
                    "c_acctbal", value(parts, 5),
                    "c_mktsegment", value(parts, 6),
                    "c_comment", value(parts, 7)
            )));
        });
    }

    private static void readOrders(
            Path path,
            int limit,
            List<UpdateEvent> updates,
            long[] timestamp
    ) throws IOException {
        readRows(path, limit, parts -> {
            requireColumns(path, parts, 2);
            String key = parts[0];
            updates.add(insert("orders", key, timestamp, attributes(
                    "o_orderkey", parts[0],
                    "o_custkey", parts[1],
                    "o_orderstatus", value(parts, 2),
                    "o_totalprice", value(parts, 3),
                    "o_orderdate", value(parts, 4),
                    "o_orderpriority", value(parts, 5),
                    "o_clerk", value(parts, 6),
                    "o_shippriority", value(parts, 7),
                    "o_comment", value(parts, 8)
            )));
        });
    }

    private static String readLineitem(
            Path path,
            int limit,
            List<UpdateEvent> updates,
            long[] timestamp
    ) throws IOException {
        final String[] firstKey = new String[] {null};
        readRows(path, limit, parts -> {
            requireColumns(path, parts, 6);
            String key = parts[0] + "#" + parts[3];
            if (firstKey[0] == null) {
                firstKey[0] = key;
            }
            updates.add(insert("lineitem", key, timestamp, attributes(
                    "l_orderkey", parts[0],
                    "l_partkey", value(parts, 1),
                    "l_suppkey", parts[2],
                    "l_linenumber", parts[3],
                    "l_quantity", value(parts, 4),
                    "l_extendedprice", parts[5],
                    "l_discount", value(parts, 6),
                    "l_tax", value(parts, 7),
                    "l_returnflag", value(parts, 8),
                    "l_linestatus", value(parts, 9),
                    "l_shipdate", value(parts, 10),
                    "l_commitdate", value(parts, 11),
                    "l_receiptdate", value(parts, 12),
                    "l_shipinstruct", value(parts, 13),
                    "l_shipmode", value(parts, 14),
                    "l_comment", value(parts, 15)
            )));
        });
        return firstKey[0];
    }

    private static UpdateEvent insert(
            String relation,
            String primaryKey,
            long[] timestamp,
            Map<String, String> attributes
    ) {
        return new UpdateEvent(
                UpdateOperation.INSERT,
                relation,
                primaryKey,
                new TupleData(relation, primaryKey, attributes),
                timestamp[0]++
        );
    }

    private static void readRows(Path path, int limit, RowConsumer consumer) throws IOException {
        if (!Files.exists(path)) {
            throw new IOException("Missing TPC-H data file: " + path.toAbsolutePath());
        }

        int count = 0;
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                consumer.accept(line.split("\\|", -1));
                count += 1;
                if (limit > 0 && count >= limit) {
                    break;
                }
            }
        }
    }

    private static void requireColumns(Path path, String[] parts, int minimumColumns) {
        if (parts.length < minimumColumns) {
            throw new IllegalArgumentException(
                    "Malformed TPC-H row in " + path.toAbsolutePath()
                            + ": expected at least " + minimumColumns
                            + " columns but found " + parts.length
            );
        }
    }

    private static String value(String[] parts, int index) {
        return index < parts.length ? parts[index] : "";
    }

    private static Map<String, String> attributes(String... keyValues) {
        Map<String, String> attributes = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            attributes.put(keyValues[i], keyValues[i + 1]);
        }
        return attributes;
    }

    private interface RowConsumer {
        void accept(String[] parts) throws IOException;
    }
}
