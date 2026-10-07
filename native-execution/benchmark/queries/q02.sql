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
    supplier.s_acctbal,
    supplier.s_name,
    nation.n_name,
    part.p_partkey,
    part.p_mfgr,
    supplier.s_address,
    supplier.s_phone,
    supplier.s_comment
FROM
    part,
    supplier,
    partsupp,
    nation,
    region
WHERE
    part.p_partkey = partsupp.ps_partkey
    AND supplier.s_suppkey = partsupp.ps_suppkey
    AND part.p_size = 15
    AND part.p_type LIKE '%BRASS'
    AND supplier.s_nationkey = nation.n_nationkey
    AND nation.n_regionkey = region.r_regionkey
    AND region.r_name = 'EUROPE'
    AND partsupp.ps_supplycost = (
        SELECT
            min(partsupp.ps_supplycost)
        FROM
            partsupp,
            supplier,
            nation,
            region
        WHERE
            part.p_partkey = partsupp.ps_partkey
            AND supplier.s_suppkey = partsupp.ps_suppkey
            AND supplier.s_nationkey = nation.n_nationkey
            AND nation.n_regionkey = region.r_regionkey
            AND region.r_name = 'EUROPE')
ORDER BY
    supplier.s_acctbal DESC,
    nation.n_name,
    supplier.s_name,
    part.p_partkey
LIMIT 100;
