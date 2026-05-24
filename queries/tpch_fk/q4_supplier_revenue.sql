SELECT s.s_suppkey, COUNT(*), SUM(l.l_extendedprice)
FROM supplier s
JOIN lineitem l ON s.s_suppkey = l.l_suppkey
GROUP BY s.s_suppkey;
