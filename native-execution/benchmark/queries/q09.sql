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
    nation,
    o_year,
    sum(amount) AS sum_profit
FROM (
    SELECT
        nation.n_name AS nation,
        extract(year FROM orders.o_orderdate) AS o_year,
        lineitem.l_extendedprice * (1 - lineitem.l_discount) - partsupp.ps_supplycost * lineitem.l_quantity AS amount
    FROM
        part,
        supplier,
        lineitem,
        partsupp,
        orders,
        nation
    WHERE
        supplier.s_suppkey = lineitem.l_suppkey
        AND partsupp.ps_suppkey = lineitem.l_suppkey
        AND partsupp.ps_partkey = lineitem.l_partkey
        AND part.p_partkey = lineitem.l_partkey
        AND orders.o_orderkey = lineitem.l_orderkey
        AND supplier.s_nationkey = nation.n_nationkey
        AND part.p_name LIKE '%green%') AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC;
