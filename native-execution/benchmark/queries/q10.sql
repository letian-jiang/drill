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
    customer.c_custkey,
    customer.c_name,
    sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS revenue,
    customer.c_acctbal,
    nation.n_name,
    customer.c_address,
    customer.c_phone,
    customer.c_comment
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    customer.c_custkey = orders.o_custkey
    AND lineitem.l_orderkey = orders.o_orderkey
    AND orders.o_orderdate >= CAST('1993-10-01' AS date)
    AND orders.o_orderdate < CAST('1994-01-01' AS date)
    AND lineitem.l_returnflag = 'R'
    AND customer.c_nationkey = nation.n_nationkey
GROUP BY
    customer.c_custkey,
    customer.c_name,
    customer.c_acctbal,
    customer.c_phone,
    nation.n_name,
    customer.c_address,
    customer.c_comment
ORDER BY
    revenue DESC
LIMIT 20;
