# 完整 TPC-H SF0.01 / Paimon plan cache 测试

当前“表标识 → 版本”直接校验实现的开关对照见 [最新性能报告](PLAN_CACHE_PERFORMANCE_REPORT.md)；下文保留改动前的历史数据。

测试日期：2026-09-29。使用仓库 `exec/java-exec/src/test/resources/queries/tpch/01.sql` 至 `22.sql`；八张完整列 Paimon 表共 86,805 行，来源为仓库自带的 SF0.01 Parquet 样本。原样本日期在 DuckDB 中显示为 15356–15363 年，导出时将年份减去 13364。Q15 使用 [子查询版本](contrib/format-paimon/src/test/resources/queries/tpch/15-subquery.sql) 替代临时视图，两处引用均展开为相同的聚合子查询。Q19 将三个 OR 分支共有的等值条件提取为 JOIN 条件，语义等价，以避开 Drill 对原写法的笛卡尔连接规划限制。

每组独立 JVM、单 Drillbit、顺序执行 22 条查询；每条执行两次，等待首次异步缓存写入，再测第二次。规划时间是 `profile.planEnd - profile.start`，端到端时间来自 `QuerySummary.runTimeMs()`。另执行一次查询获取结果，按行排序后计算 SHA-256 指纹。关闭和开启缓存的 22 个指纹全部相同，且各查询行数与 DuckDB 独立执行的结果一致；行数已固化为测试断言。Q4 的五个具体计数也与 DuckDB 一致，并固化了正确结果的指纹断言。

| Q | 行数 | 关闭规划 ms | 开启规划 ms | 关闭总耗时 ms | 开启总耗时 ms | 第二次命中 |
| ---: | ---: | ---: | ---: | ---: | ---: | :---: |
| 01 | 4 | 185 | 103 | 531 | 517 | 是 |
| 02 | 2 | 448 | 140 | 1229 | 831 | 是 |
| 03 | 10 | 263 | 103 | 503 | 359 | 是 |
| 04 | 5 | 164 | 71 | 311 | 261 | 是 |
| 05 | 5 | 408 | 94 | 1101 | 821 | 是 |
| 06 | 1 | 140 | 93 | 176 | 189 | 是 |
| 07 | 4 | 416 | 87 | 953 | 504 | 是 |
| 08 | 2 | 649 | 123 | 1210 | 641 | 是 |
| 09 | 175 | 318 | 126 | 743 | 522 | 是 |
| 10 | 20 | 252 | 76 | 619 | 429 | 是 |
| 11 | 297 | 459 | 88 | 857 | 343 | 是 |
| 12 | 2 | 127 | 88 | 295 | 287 | 是 |
| 13 | 33 | 112 | 64 | 250 | 198 | 是 |
| 14 | 1 | 127 | 64 | 214 | 209 | 是 |
| 15 | 1 | 187 | 70 | 345 | 296 | 是 |
| 16 | 288 | 337 | 87 | 509 | 254 | 是 |
| 17 | 1 | 176 | 85 | 339 | 286 | 是 |
| 18 | 2 | 207 | 81 | 481 | 359 | 是 |
| 19 | 1 | 191 | 76 | 436 | 287 | 是 |
| 20 | 6 | 251 | 81 | 551 | 751 | 是 |
| 21 | 2 | 395 | 117 | 1810 | 1539 | 是 |
| 22 | 7 | 181 | 76 | 306 | 234 | 是 |

22 条的第二次执行全部成功，且 **22 条全部命中**。规划耗时合计由 **5,993 ms** 降为 **1,993 ms**（降 66.7%），端到端合计由 **13,769 ms** 降为 **10,117 ms**（降 26.5%）。Q15 单独的规划耗时由 187 ms 降为 70 ms。首次执行的 22 条规划合计为关闭 **7,144 ms**、开启 **7,612 ms**；端到端分别为 **17,286 ms**、**18,003 ms**。这是单次顺序跑分，首次查询和聚合耗时都可能受 JVM 预热影响，不应当作稳定吞吐量结论。

### 原先四条未命中的根因与修复

旧版参数化后 Q06、Q14、Q19、Q22 无法缓存。`SqlBoundDynamicParam` 虽保存原始 `SqlLiteral`，但 Calcite 校验器仍把它视为无类型 `?`；原先直到 SQL→Rex 转换时才恢复类型。Q06 的 `BETWEEN` 算术边界、Q19 的数量区间算术、Q22 的 `SUBSTRING` 位置参数因此缺少类型上下文；Q14 的 `CASE` 分支类型推导则查询了尚未登记的参数类型。现在 `DrillSqlValidator` 在校验阶段为绑定参数提供原字面量类型，四条的第二次执行均已命中，结果指纹与关闭缓存组一致。旧诊断见 [日志](artifacts/tpch-full-cache-misses-diagnostic.log)。

原始 Q15 引用临时视图 `revenue0`，当前 `PlanCache.ContextSnapshot.resolve` 无法展开并验证其底表版本，因此拒绝写入。改为两个等价的聚合子查询后，底表均可解析，第二次执行命中；结果指纹与原视图版本相同。

## 测试中修复的问题

- Paimon DATE 列内部读取值为 epoch-day 整数，输出转换器原先按 `LocalDate` 强转，导致多个查询读取失败；现显式转换。
- `ExprToRex.findField` 原先把插件投影字段名 `*` 当表达式解析，导致相关子查询规划失败；现先按原始字段名查找。
- Paimon 连续下推过滤条件时，后一次覆盖前一次，Q4 的普通规划因此漏掉日期过滤；现将条件用 AND 合并。修复前 Q4 的五组计数约为 2700，正确结果为 118、119、114、93、106。

## 复现

```sh
mkdir -p artifacts/tpch-sf001-full-csv
duckdb < artifacts/tpch-sf001-full-export.sql
for enabled in false true; do
  mvn -pl contrib/format-paimon -am \
    -Dtest=PaimonTpchFullQueriesTest \
    -Dsurefire.failIfNoSpecifiedTests=false \
    -Ddrill.tpch.full.csv.dir="$PWD/artifacts/tpch-sf001-full-csv" \
    -Ddrill.tpch.full.query.dir="$PWD/exec/java-exec/src/test/resources/queries/tpch" \
    -Ddrill.tpch.full.cache.enabled="$enabled" package
done
```

原始输出：[关闭](artifacts/tpch-full-off-q15-subquery.log)、[开启](artifacts/tpch-full-on-q15-subquery.log)。行数和跨组指纹检查不代表与独立引擎逐值核对。
