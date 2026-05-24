SELECT c.c_custkey, COUNT(*), SUM(l.l_extendedprice)
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON o.o_orderkey = l.l_orderkey
GROUP BY c.c_custkey;
