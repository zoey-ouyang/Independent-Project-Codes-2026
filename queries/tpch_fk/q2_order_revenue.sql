SELECT o.o_orderkey, COUNT(*), SUM(l.l_extendedprice)
FROM orders o
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
GROUP BY o.o_orderkey;
