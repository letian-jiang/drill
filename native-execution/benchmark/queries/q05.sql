-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
SELECT
    nation.n_name,
    sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    customer.c_custkey = orders.o_custkey
    AND lineitem.l_orderkey = orders.o_orderkey
    AND lineitem.l_suppkey = supplier.s_suppkey
    AND customer.c_nationkey = supplier.s_nationkey
    AND supplier.s_nationkey = nation.n_nationkey
    AND nation.n_regionkey = region.r_regionkey
    AND region.r_name = 'ASIA'
    AND orders.o_orderdate >= CAST('1994-01-01' AS date)
    AND orders.o_orderdate < CAST('1995-01-01' AS date)
GROUP BY
    nation.n_name
ORDER BY
    revenue DESC;
