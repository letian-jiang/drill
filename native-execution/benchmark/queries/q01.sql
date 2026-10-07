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
    lineitem.l_returnflag,
    lineitem.l_linestatus,
    sum(lineitem.l_quantity) AS sum_qty,
    sum(lineitem.l_extendedprice) AS sum_base_price,
    sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS sum_disc_price,
    sum(lineitem.l_extendedprice * (1 - lineitem.l_discount) * (1 + lineitem.l_tax)) AS sum_charge,
    avg(lineitem.l_quantity) AS avg_qty,
    avg(lineitem.l_extendedprice) AS avg_price,
    avg(lineitem.l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    lineitem
WHERE
    lineitem.l_shipdate <= CAST('1998-09-02' AS date)
GROUP BY
    lineitem.l_returnflag,
    lineitem.l_linestatus
ORDER BY
    lineitem.l_returnflag,
    lineitem.l_linestatus;
