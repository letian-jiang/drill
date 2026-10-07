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
    lineitem.l_shipmode,
    sum(
        CASE WHEN orders.o_orderpriority = '1-URGENT'
            OR orders.o_orderpriority = '2-HIGH' THEN
            1
        ELSE
            0
        END) AS high_line_count,
    sum(
        CASE WHEN orders.o_orderpriority <> '1-URGENT'
            AND orders.o_orderpriority <> '2-HIGH' THEN
            1
        ELSE
            0
        END) AS low_line_count
FROM
    orders,
    lineitem
WHERE
    orders.o_orderkey = lineitem.l_orderkey
    AND lineitem.l_shipmode IN ('MAIL', 'SHIP')
    AND lineitem.l_commitdate < lineitem.l_receiptdate
    AND lineitem.l_shipdate < lineitem.l_commitdate
    AND lineitem.l_receiptdate >= CAST('1994-01-01' AS date)
    AND lineitem.l_receiptdate < CAST('1995-01-01' AS date)
GROUP BY
    lineitem.l_shipmode
ORDER BY
    lineitem.l_shipmode;
