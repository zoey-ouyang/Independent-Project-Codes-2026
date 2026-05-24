SELECT n.n_nationkey, COUNT(*), SUM(l.l_extendedprice)
FROM nation n
JOIN customer c ON n.n_nationkey = c.c_nationkey
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
GROUP BY n.n_nationkey;
