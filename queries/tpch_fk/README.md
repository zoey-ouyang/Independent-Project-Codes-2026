# TPC-H FK-DAG Query Workload

These queries are representative acyclic foreign-key joins derived from the TPC-H schema.

They intentionally stay within the prototype's supported query class:

- select-project-join-aggregate style
- joins backed by primary-key / foreign-key edges
- acyclic FK DAGs
- grouped `COUNT(*)`
- grouped `SUM(relation.column)`

The prototype does not attempt to execute the full official TPC-H query templates because many of them include nested subqueries, non-FK predicates, date filters, arithmetic expressions, ordering, and other SQL features outside the current reproduction scope.
