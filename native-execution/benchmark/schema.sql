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
CREATE OR REPLACE SCHEMA (`r_regionkey` INTEGER, `r_name` VARCHAR, `r_comment` VARCHAR) FOR TABLE dfs.tpch.region;
CREATE OR REPLACE SCHEMA (`n_nationkey` INTEGER, `n_name` VARCHAR, `n_regionkey` INTEGER, `n_comment` VARCHAR) FOR TABLE dfs.tpch.nation;
CREATE OR REPLACE SCHEMA (`s_suppkey` BIGINT, `s_name` VARCHAR, `s_address` VARCHAR, `s_nationkey` INTEGER, `s_phone` VARCHAR, `s_acctbal` DECIMAL(15,2), `s_comment` VARCHAR) FOR TABLE dfs.tpch.supplier;
CREATE OR REPLACE SCHEMA (`c_custkey` BIGINT, `c_name` VARCHAR, `c_address` VARCHAR, `c_nationkey` INTEGER, `c_phone` VARCHAR, `c_acctbal` DECIMAL(15,2), `c_mktsegment` VARCHAR, `c_comment` VARCHAR) FOR TABLE dfs.tpch.customer;
CREATE OR REPLACE SCHEMA (`p_partkey` BIGINT, `p_name` VARCHAR, `p_mfgr` VARCHAR, `p_brand` VARCHAR, `p_type` VARCHAR, `p_size` INTEGER, `p_container` VARCHAR, `p_retailprice` DECIMAL(15,2), `p_comment` VARCHAR) FOR TABLE dfs.tpch.part;
CREATE OR REPLACE SCHEMA (`ps_partkey` BIGINT, `ps_suppkey` BIGINT, `ps_availqty` BIGINT, `ps_supplycost` DECIMAL(15,2), `ps_comment` VARCHAR) FOR TABLE dfs.tpch.partsupp;
CREATE OR REPLACE SCHEMA (`o_orderkey` BIGINT, `o_custkey` BIGINT, `o_orderstatus` VARCHAR, `o_totalprice` DECIMAL(15,2), `o_orderdate` DATE, `o_orderpriority` VARCHAR, `o_clerk` VARCHAR, `o_shippriority` INTEGER, `o_comment` VARCHAR) FOR TABLE dfs.tpch.orders;
CREATE OR REPLACE SCHEMA (`l_orderkey` BIGINT, `l_partkey` BIGINT, `l_suppkey` BIGINT, `l_linenumber` BIGINT, `l_quantity` DECIMAL(15,2), `l_extendedprice` DECIMAL(15,2), `l_discount` DECIMAL(15,2), `l_tax` DECIMAL(15,2), `l_returnflag` VARCHAR, `l_linestatus` VARCHAR, `l_shipdate` DATE, `l_commitdate` DATE, `l_receiptdate` DATE, `l_shipinstruct` VARCHAR, `l_shipmode` VARCHAR, `l_comment` VARCHAR) FOR TABLE dfs.tpch.lineitem;
