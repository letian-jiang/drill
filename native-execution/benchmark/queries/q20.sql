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
    supplier.s_name,
    supplier.s_address
FROM
    supplier,
    nation
WHERE
    supplier.s_suppkey IN (
        SELECT
            partsupp.ps_suppkey
        FROM
            partsupp
        WHERE
            partsupp.ps_partkey IN (
                SELECT
                    part.p_partkey
                FROM
                    part
                WHERE
                    part.p_name LIKE 'forest%')
                AND partsupp.ps_availqty > (
                    SELECT
                        0.5 * sum(lineitem.l_quantity)
                    FROM
                        lineitem
                    WHERE
                        lineitem.l_partkey = partsupp.ps_partkey
                        AND lineitem.l_suppkey = partsupp.ps_suppkey
                        AND lineitem.l_shipdate >= CAST('1994-01-01' AS date)
                        AND lineitem.l_shipdate < CAST('1995-01-01' AS date)))
            AND supplier.s_nationkey = nation.n_nationkey
            AND nation.n_name = 'CANADA'
        ORDER BY
            supplier.s_name;
