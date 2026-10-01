-- TPC-H Q15 with derived tables instead of a temporary view.
select
  s.s_suppkey,
  s.s_name,
  s.s_address,
  s.s_phone,
  r.total_revenue
from
  cp.`tpch/supplier.parquet` s,
  (
    select
      l_suppkey as supplier_no,
      sum(l_extendedprice * (1 - l_discount)) as total_revenue
    from
      cp.`tpch/lineitem.parquet`
    where
      l_shipdate >= date '1993-05-01'
      and l_shipdate < date '1993-05-01' + interval '3' month
    group by
      l_suppkey
  ) r
where
  s.s_suppkey = r.supplier_no
  and r.total_revenue = (
    select
      max(total_revenue)
    from
      (
        select
          l_suppkey as supplier_no,
          sum(l_extendedprice * (1 - l_discount)) as total_revenue
        from
          cp.`tpch/lineitem.parquet`
        where
          l_shipdate >= date '1993-05-01'
          and l_shipdate < date '1993-05-01' + interval '3' month
        group by
          l_suppkey
      ) revenue
  )
order by
  s.s_suppkey;
