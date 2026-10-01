# Paimon TPC-H SF0.01 plan cache benchmark

测试日期：2026-09-29。本地单 Drillbit，每组使用独立 JVM。数据来自仓库自带的 TPC-H 样本 Parquet，导出后加载到三张 Paimon append 表：`customer` 1,500 行、`orders` 15,000 行、`lineitem` 60,175 行。只加载本次查询所需的列，因此这是 SF0.01 **行数与取值规模**测试，不是完整八表、完整列的 TPC-H 规范跑分。

原 Parquet 的日期在 DuckDB 中显示为 15356–15363 年；导出时将年份减去 13364，得到 1992–1999 年，月日不变。导出 SQL 见 [artifacts/tpch-sf001-export.sql](artifacts/tpch-sf001-export.sql)。测试将 Paimon 标量结果与同一 TSV 数据在 DuckDB 中算出的预期值比较。

每个模板先执行三条无关的 `count(*)` 查询预热，测一次该模板首次规划，等待异步缓存写入，然后交替执行三个不同字面量版本共 12 次。`planMs = profile.planEnd - profile.start`；`elapsedMs` 来自 `QuerySummary.runTimeMs()`。表中为后续 12 次的中位数，单位毫秒。运行顺序为开→关、关→开；每行对应独立 JVM。

下表采集于本次正确性修复之前；三个模板当时已通过结果校验。修复后的 Q6 和复杂查询仅复核了正确性，尚未重新采集性能对照。

| 模板 | 轮次 | 关闭：规划 / 端到端 | 开启：规划 / 端到端 | 开启组命中 | 规划降幅 | 端到端降幅 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `lineitem` 日期过滤计数，Q1 过滤条件简化版 | 1 | 126 / 201 | 57 / 117 | 12/12 | 55% | 42% |
| 同上 | 2 | 119 / 186 | 72 / 133 | 12/12 | 39% | 28% |
| `customer` 市场分组过滤计数 | 1 | 106 / 150 | 52 / 91 | 10/12 | 51% | 39% |
| 同上 | 2 | 103 / 144 | 56 / 103 | 10/12 | 46% | 28% |
| `orders` 日期过滤计数 | 1 | 130 / 196 | 52 / 104 | 12/12 | 60% | 47% |
| 同上 | 2 | 92 / 131 | 67 / 114 | 12/12 | 27% | 13% |

纳入表格的三个模板，其三个字面量版本在开关两组都通过预期值校验。`customer` 的两个首次新字面量版本没有命中，此后命中；该模板尚未实现三个取值共用一个缓存条目。首次规划耗时波动较大，不能据此得出首次查询开销的稳定结论。原始输出：

- 第一轮：[开启](artifacts/tpch-sf001-on-1.log)、[关闭](artifacts/tpch-sf001-off-1.log)
- 第二轮：[关闭](artifacts/tpch-sf001-off-2.log)、[开启](artifacts/tpch-sf001-on-2.log)

## 测试期间发现并修复的正确性问题

1. Q6 风格的日期、折扣、数量多谓词计数，DuckDB 和关闭缓存的 Drill 都返回 **1,214**；修复前开启缓存后首次执行就返回 **384**，且命中数为 0。逐项比较发现 `0.05` 的动态参数被 Calcite 推断为 `ANY`，`RexBuilder.makeLiteral(value, ANY)` 生成了 `BIGINT` 字面量。现在遇到 `ANY` 时改用原 SQL 字面量类型构造 `RexLiteral`，保留小数精度。修复后首次执行与后续命中均返回 **1,214**，后续查询各命中一次。见 [修复前开启](artifacts/tpch-sf001-q6-on.log)、[修复前关闭](artifacts/tpch-sf001-q6-off.log)、[逐项谓词修复后](artifacts/tpch-sf001-q6-components-fixed.log)、[完整查询修复后](artifacts/tpch-sf001-correctness-fixed-on.log)。该模板尚未重新做稳定性能对照，因此仍不参与上表收益结论。
2. 带过滤的 `customer`–`orders` join 计数在 DuckDB 为 **1,693**，修复前关闭缓存的 Drill 返回 **0**。其 scan 只投影 join key，却需要在被裁掉的列上执行 Paimon 谓词。现在 Paimon 的读取投影附加谓词引用列，ScanLifecycle 继续只输出查询所需列。修复后单侧过滤和双侧过滤 join 在关闭、开启缓存时均与 DuckDB 对齐；双侧过滤返回 **1,693**。Q1/Q3 风格查询分别返回 4/10 行，分组总行数 **57,600** 和 Q3 前十订单号之和 **306,743** 也对齐。见 [修复前](artifacts/tpch-sf001-join-off.log)、[join 修复后](artifacts/tpch-sf001-join-projection-fixed.log)、[复杂查询开启](artifacts/tpch-sf001-complex-values-on.log)、[复杂查询关闭](artifacts/tpch-sf001-complex-values-off.log)。这些复杂查询尚未做稳定性能对照。

## 复现

从仓库根目录执行：

```sh
mkdir -p artifacts/tpch-sf001-csv
duckdb < artifacts/tpch-sf001-export.sql
mvn -pl contrib/format-paimon -am \
  -Dtest=PaimonTpchPlanCacheBenchmarkTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Ddrill.tpch.csv.dir="$PWD/artifacts/tpch-sf001-csv" \
  -Ddrill.tpch.benchmark.samples=12 \
  -Ddrill.plan.cache.benchmark.enabled=true package
```

把最后一个参数改为 `false` 运行关闭组。针对 Q6 的首次参数化错误，可以将 `-Dtest` 改为 `PaimonTpchPlanCacheBenchmarkTest#diagnoseQ6`，并加 `-Ddrill.tpch.q6.diagnostic=true`。

这些结果只覆盖单机、小规模、顺序执行的过滤计数；不代表 SF0.01 完整 TPC-H 查询或多 Drillbit、高并发的收益。端到端时间包含扫描与执行；规划时间包含 Query Profile 中规划前的部分准备工作，不等同于纯 Calcite 优化时间。
