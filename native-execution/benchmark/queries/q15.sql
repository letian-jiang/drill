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
WITH revenue AS (
    SELECT
        lineitem.l_suppkey AS supplier_no,
        sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS total_revenue
    FROM
        lineitem
    WHERE
        lineitem.l_shipdate >= CAST('1996-01-01' AS date)
      AND lineitem.l_shipdate < CAST('1996-04-01' AS date)
    GROUP BY
        supplier_no
)
SELECT
    supplier.s_suppkey,
    supplier.s_name,
    supplier.s_address,
    supplier.s_phone,
    revenue.total_revenue
FROM
    supplier,
    revenue
WHERE
    supplier.s_suppkey = revenue.supplier_no
    AND revenue.total_revenue = (
        SELECT
            max(revenue.total_revenue)
        FROM revenue)
ORDER BY
    supplier.s_suppkey;
