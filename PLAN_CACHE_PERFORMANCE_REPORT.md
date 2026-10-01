# Paimon Plan Cache 性能报告

插件适配步骤与当前命中检查方法见 [Storage plugin 接入说明](PLAN_CACHE_PLUGIN_GUIDE.md)。

测试日期：2026-09-29。基于当前“物理表标识 → 表版本”直接校验实现，在本地单 Drillbit 上重新运行 YCSB 风格负载和 TPC-H SF0.01。缓存开启、关闭两组各使用独立 JVM，顺序执行。规划耗时取查询 profile 的 `planEnd - start`，端到端耗时取 `QuerySummary.runTimeMs()`；均不包含 Maven 构建和建表加载时间。

## 结果摘要

| 负载 | 样本与命中 | 关闭缓存规划 | 开启缓存规划 | 规划降幅 | 关闭缓存端到端 | 开启缓存端到端 | 端到端降幅 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| YCSB 风格点查 | 后续 9 次，9/9 命中；中位数 | 126 ms | 18 ms | 85.7% | 216 ms | 65 ms | 69.9% |
| YCSB 风格 50 行范围扫描 | 后续 9 次，9/9 命中；中位数 | 115 ms | 15 ms | 87.0% | 155 ms | 60 ms | 61.3% |
| YCSB 风格有序扫描 | 后续 9 次，9/9 命中；中位数 | 93 ms | 12 ms | 87.1% | 143 ms | 64 ms | 55.2% |
| TPC-H SF0.01，22 条查询 | 每条第二次执行，22/22 命中；合计 | 6,091 ms | 885 ms | 85.5% | 13,989 ms | 9,566 ms | 31.6% |

TPC-H 的规划合计相当于 6.88 倍加速，端到端合计相当于 1.46 倍加速。两组各执行一轮，数据应视为本地观察值，而非稳定吞吐量或 TPC-H 官方成绩。

## YCSB 风格负载

测试生成 10,000 行单 bucket Paimon 主键表，含一个 `ycsb_key` 和十个 100 字节字符串列。点查返回两列，范围查询返回 50 行，有序扫描含固定 `ORDER BY ... LIMIT 10`。每类查询先执行一次冷查询并等待异步缓存写入，然后在两个 Drill 连接之间交替执行三个不同字面量版本，各采样 9 次。每个版本还核对预期行值和结果指纹。缓存开启组的 27 次后续执行全部命中，关闭组命中数为零。

三个模板的首次查询规划耗时，关闭/开启组依次是点查 1,168/1,325 ms、范围扫描 134/173 ms、有序扫描 159/229 ms。点查首次查询明显包含 JVM 与插件预热；每类只有一个冷样本，不能据此估计稳定的首次规划额外开销。

原始日志：[关闭缓存](artifacts/ycsb-current-off.log)、[开启缓存](artifacts/ycsb-current-on.log)。生成数据和查询的代码见 [PaimonYcsbPlanCacheBenchmarkTest](contrib/format-paimon/src/test/java/org/apache/drill/exec/store/paimon/PaimonYcsbPlanCacheBenchmarkTest.java)。本负载只模拟 YCSB 风格的读取 SQL，没有运行 YCSB 客户端，也没有 Zipf 分布、更新或并发吞吐量测试。

## 完整 TPC-H SF0.01 查询集

测试从仓库自带 SF0.01 Parquet 样本导出数据，加载成八张完整列的 Paimon 表，共 86,805 行；数据导出时修正了样本日期的年份。执行仓库中的 Q01–Q22，其中 Q15 采用等价的子查询版本，Q19 提取等值 JOIN 条件以适配 Drill 规划。每条查询执行两次，首次执行后等待异步缓存写入；下面列出第二次执行的数据。另执行一次获取完整结果，按行排序计算 SHA-256。关闭与开启缓存的 22 个指纹全部相同，且各查询行数符合测试中的 DuckDB 预期断言。

| Q | 规划：关闭 / 开启 ms | 端到端：关闭 / 开启 ms | 命中 |
| ---: | ---: | ---: | :---: |
| 01 | 197 / 37 | 413 / 606 | 是 |
| 02 | 407 / 107 | 1,130 / 1,134 | 是 |
| 03 | 255 / 33 | 490 / 322 | 是 |
| 04 | 214 / 25 | 375 / 230 | 是 |
| 05 | 362 / 46 | 1,068 / 788 | 是 |
| 06 | 129 / 14 | 191 / 126 | 是 |
| 07 | 435 / 67 | 886 / 521 | 是 |
| 08 | 478 / 61 | 1,039 / 570 | 是 |
| 09 | 272 / 91 | 667 / 528 | 是 |
| 10 | 229 / 32 | 656 / 387 | 是 |
| 11 | 304 / 44 | 564 / 322 | 是 |
| 12 | 125 / 20 | 288 / 226 | 是 |
| 13 | 251 / 19 | 467 / 159 | 是 |
| 14 | 280 / 21 | 397 / 167 | 是 |
| 15 | 226 / 28 | 467 / 271 | 是 |
| 16 | 510 / 25 | 700 / 226 | 是 |
| 17 | 201 / 27 | 384 / 220 | 是 |
| 18 | 226 / 28 | 492 / 401 | 是 |
| 19 | 131 / 18 | 342 / 271 | 是 |
| 20 | 285 / 33 | 588 / 352 | 是 |
| 21 | 340 / 85 | 1,982 / 1,527 | 是 |
| 22 | 234 / 24 | 403 / 212 | 是 |

第二次执行的规划耗时中位数从 253 ms 降至 30 ms，端到端中位数从 491 ms 降至 322 ms。Q01 与 Q02 虽然规划更快，但本轮端到端耗时没有下降；执行时间及单轮运行波动可能掩盖规划收益，不应从这两个点推断稳定回归。

首次执行的 22 条查询均未命中。关闭/开启缓存的规划耗时合计为 7,161/8,226 ms，端到端合计为 17,935/19,164 ms；开启组分别高 14.9% 和 6.9%。首次执行需要收集表依赖和参数化，但本次只有各一轮，且顺序与 JVM 预热会影响数值，尚不能把差额全部归因于缓存逻辑。缓存适合模板会重复执行的场景。

原始日志：[关闭缓存](artifacts/tpch-full-current-off.log)、[开启缓存](artifacts/tpch-full-current-on.log)。数据导出脚本为 [tpch-sf001-full-export.sql](artifacts/tpch-sf001-full-export.sql)，测试代码为 [PaimonTpchFullQueriesTest](contrib/format-paimon/src/test/java/org/apache/drill/exec/store/paimon/PaimonTpchFullQueriesTest.java)。完整的旧实现结果见 [TPC-H 历史报告](PLAN_CACHE_TPCH_FULL_BENCHMARK.md)，不与本次开关组混算。

## 复现

先按照导出脚本生成 `artifacts/tpch-sf001-full-csv`。从仓库根目录运行：

```sh
for enabled in false true; do
  mvn -pl contrib/format-paimon -am \
    -Dtest=PaimonYcsbPlanCacheBenchmarkTest \
    -Dsurefire.failIfNoSpecifiedTests=false \
    -Ddrill.ycsb.cache.enabled="$enabled" \
    -Ddrill.ycsb.samples=9 package
done

for enabled in false true; do
  mvn -pl contrib/format-paimon -am \
    -Dtest=PaimonTpchFullQueriesTest \
    -Dsurefire.failIfNoSpecifiedTests=false \
    -Ddrill.tpch.full.csv.dir="$PWD/artifacts/tpch-sf001-full-csv" \
    -Ddrill.tpch.full.query.dir="$PWD/exec/java-exec/src/test/resources/queries/tpch" \
    -Ddrill.tpch.full.cache.enabled="$enabled" package
done
```

每条命令都以 `BUILD SUCCESS` 结束；TPC-H 两组均报告 `queries=22 failures=0`。测试仅覆盖本地单 Drillbit、顺序执行、Paimon 表和已展示的 SQL。没有测跨 Drillbit、混合更新、并发吞吐量或其他 storage plugin。
