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
    orders.o_orderpriority,
    count(*) AS order_count
FROM
    orders
WHERE
    orders.o_orderdate >= CAST('1993-07-01' AS date)
    AND orders.o_orderdate < CAST('1993-10-01' AS date)
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem
        WHERE
            lineitem.l_orderkey = orders.o_orderkey
            AND lineitem.l_commitdate < lineitem.l_receiptdate)
GROUP BY
    orders.o_orderpriority
ORDER BY
    orders.o_orderpriority;
