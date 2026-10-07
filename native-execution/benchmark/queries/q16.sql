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
    part.p_brand,
    part.p_type,
    part.p_size,
    count(DISTINCT partsupp.ps_suppkey) AS supplier_cnt
FROM
    partsupp,
    part
WHERE
    part.p_partkey = partsupp.ps_partkey
    AND part.p_brand <> 'Brand#45'
    AND part.p_type NOT LIKE 'MEDIUM POLISHED%'
    AND part.p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND partsupp.ps_suppkey NOT IN (
        SELECT
            supplier.s_suppkey
        FROM
            supplier
        WHERE
            supplier.s_comment LIKE '%Customer%Complaints%')
GROUP BY
    part.p_brand,
    part.p_type,
    part.p_size
ORDER BY
    supplier_cnt DESC,
    part.p_brand,
    part.p_type,
    part.p_size;
