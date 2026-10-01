-- Run from the repository root with:
-- mkdir -p artifacts/tpch-sf001-csv
-- duckdb < artifacts/tpch-sf001-export.sql
-- The bundled Parquet date values display as years 15356-15363 in DuckDB.
-- Subtracting 13364 from the year restores the TPC-H 1992-1999 date range.

COPY (
  SELECT c_custkey, c_mktsegment
  FROM 'contrib/data/tpch-sample-data/target/classes/tpch/customer.parquet'
) TO 'artifacts/tpch-sf001-csv/customer.tsv' (DELIMITER E'\t', HEADER false);

COPY (
  SELECT o_orderkey, o_custkey,
    make_date(year(o_orderdate) - 13364, month(o_orderdate), day(o_orderdate)),
    o_shippriority
  FROM 'contrib/data/tpch-sample-data/target/classes/tpch/orders.parquet'
) TO 'artifacts/tpch-sf001-csv/orders.tsv' (DELIMITER E'\t', HEADER false);

COPY (
  SELECT l_orderkey,
    make_date(year(l_shipdate) - 13364, month(l_shipdate), day(l_shipdate)),
    l_quantity, l_extendedprice, l_discount, l_returnflag, l_linestatus
  FROM 'contrib/data/tpch-sample-data/target/classes/tpch/lineitem.parquet'
) TO 'artifacts/tpch-sf001-csv/lineitem.tsv' (DELIMITER E'\t', HEADER false);
