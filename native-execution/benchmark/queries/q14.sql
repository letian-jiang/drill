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
    100.00 * sum(
        CASE WHEN part.p_type LIKE 'PROMO%' THEN
            lineitem.l_extendedprice * (1 - lineitem.l_discount)
        ELSE
            0
        END) / sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS promo_revenue
FROM
    lineitem,
    part
WHERE
    lineitem.l_partkey = part.p_partkey
    AND lineitem.l_shipdate >= date '1995-09-01'
    AND lineitem.l_shipdate < CAST('1995-10-01' AS date);
