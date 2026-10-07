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
    lineitem.l_orderkey,
    sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS revenue,
    orders.o_orderdate,
    orders.o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    customer.c_mktsegment = 'BUILDING'
    AND customer.c_custkey = orders.o_custkey
    AND lineitem.l_orderkey = orders.o_orderkey
    AND orders.o_orderdate < CAST('1995-03-15' AS date)
    AND lineitem.l_shipdate > CAST('1995-03-15' AS date)
GROUP BY
    lineitem.l_orderkey,
    orders.o_orderdate,
    orders.o_shippriority
ORDER BY
    revenue DESC,
    orders.o_orderdate
LIMIT 10;
