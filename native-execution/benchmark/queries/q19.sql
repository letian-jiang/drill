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
    sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS revenue
FROM
    lineitem,
    part
WHERE part.p_partkey = lineitem.l_partkey AND ((part.p_brand = 'Brand#12'
    AND part.p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND lineitem.l_quantity >= 1
    AND lineitem.l_quantity <= 1 + 10
    AND part.p_size BETWEEN 1 AND 5
    AND lineitem.l_shipmode IN ('AIR', 'AIR REG')
    AND lineitem.l_shipinstruct = 'DELIVER IN PERSON')
    OR (part.p_brand = 'Brand#23'
        AND part.p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND lineitem.l_quantity >= 10
        AND lineitem.l_quantity <= 10 + 10
        AND part.p_size BETWEEN 1 AND 10
        AND lineitem.l_shipmode IN ('AIR', 'AIR REG')
        AND lineitem.l_shipinstruct = 'DELIVER IN PERSON')
    OR (part.p_brand = 'Brand#34'
        AND part.p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND lineitem.l_quantity >= 20
        AND lineitem.l_quantity <= 20 + 10
        AND part.p_size BETWEEN 1 AND 15
        AND lineitem.l_shipmode IN ('AIR', 'AIR REG')
        AND lineitem.l_shipinstruct = 'DELIVER IN PERSON')
);
