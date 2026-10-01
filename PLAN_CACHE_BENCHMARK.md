# Plan cache 初步收益测试

## 2026-09-29 当前实现重测

当前实现包含 Paimon `uuid() + schema id` 版本检查、命中时重新解析来源和重建 splits，以及有效选项指纹。仍使用本地单 Drillbit、三行 Paimon append table、每组 5 次预热、1 次首次查询和 20 次同模板查询；每行均为独立 JVM。第一轮按“开→关”，第二轮按“关→开”运行。规划耗时为 Query Profile 的 `planEnd - start`，端到端耗时来自测试客户端，单位 ms。

| 轮次 | 缓存 | 首次规划 | 后续规划中位数 / P90 | 端到端中位数 / P90 | 后续命中数 |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | 开 | 123 | 59 / 72 | 96 / 116 | 20/20 |
| 1 | 关 | 98 | 156 / 213 | 218 / 325 | 0/20 |
| 2 | 关 | 115 | 123 / 257 | 173 / 328 | 0/20 |
| 2 | 开 | 134 | 54 / 71 | 94 / 114 | 20/20 |

两轮的规划中位数分别下降约 62% 和 56%，端到端中位数分别下降约 56% 和 46%。关闭组的 P90 波动明显，本地三行表和跨 JVM 顺序对照不足以预测大表或高并发收益。第一轮开启组尚未包含将选项指纹压缩为 SHA-256 的微小调整；第二轮已包含，该调整不改变指纹所覆盖的选项。原始输出保存在 `/tmp/drill-plan-cache-bench-current-{on,off}-{1,2}.log`。

## 2026-09-28 历史基准

测试日期：2026-09-28。本地单 Drillbit 测试集群，查询通过客户端执行，Paimon 为测试生成的三行 append table；Parquet 为 CTAS 生成的三行表。每组先运行 5 次预热查询，再运行 1 次待测模板的首次查询和 20 次同模板查询；后续查询的字面量在 1、2、3 之间轮换。每组使用独立 JVM。

`规划耗时`取 Query Profile 的 `planEnd - start`，包含规划前的部分准备工作，不等同于纯 Calcite 优化耗时。`端到端耗时`取测试客户端 `QuerySummary.runTimeMs()`。表中是后续 20 次查询的中位数，单位 ms；首次查询单独列出。

## 命中时移除重复 SQL 校验后

移除缓存命中路径的 `parser.validate(sqlNode)` 后，按“开→关”和“关→开”各运行一轮独立 JVM 对照：

| 工作负载 | 轮次 | 缓存 | 首次规划 | 后续规划中位数 / P90 | 端到端中位数 / P90 | 后续命中数 |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Paimon `WHERE id = ?` | 1 | 开 | 174 | 9 / 13 | 51 / 60 | 20/20 |
| Paimon `WHERE id = ?` | 1 | 关 | 102 | 110 / 141 | 157 / 189 | 0/20 |
| Paimon `WHERE id = ?` | 2 | 关 | 99 | 91 / 118 | 124 / 154 | 0/20 |
| Paimon `WHERE id = ?` | 2 | 开 | 132 | 9 / 11 | 48 / 57 | 20/20 |

两轮后续规划中位数分别降低约 92% 和 90%，端到端分别降低约 68% 和 61%。首次规划“开”分别比“关”多 72 和 33 ms；每组使用独立 JVM，当前数据不能把差值归因于某个阶段。此结果验证重复完整校验是原命中路径的主要成本，但未覆盖权限撤销和视图定义变更后的缓存失效。

## 加入轻量来源检查后

缓存项只接纳全限定的直接文件系统表；命中时比较插件配置，用首次规划解析表时的文件系统列举工作区和表目录，并检查同名视图及 Paimon 元数据目录。临时分段计时中，整段来源检查通常约 2–3 ms；其中插件查询约 1–2 ms，目录列举合计约 1 ms。`FileSystem.access` 在本地基准里每个目录需约 10–20 ms，故改用目录列举。目录列举无法证明每个数据文件的权限，启用 Drill impersonation 时缓存直接关闭。该性能结论仅覆盖本地测试文件系统；大目录或远程文件系统需要单独测试。

| 工作负载 | 缓存 | 首次规划 | 后续规划中位数 / P90 | 端到端中位数 / P90 | 后续命中数 |
| --- | --- | ---: | ---: | ---: | ---: |
| Paimon `WHERE id = ?` | 开，轻量检查 | 115 | 13 / 15 | 58 / 72 | 20/20 |
| Paimon `WHERE id = ?` | 开，移除打点后的最终代码 | 140 | 11 / 14 | 50 / 58 | 20/20 |

第一行是带临时计时的单轮结果，第二行是移除打点、增加空目录保护并关闭 impersonation 缓存后的最终代码。最终代码命中规划中位数 11 ms，仍接近无校验版的 9 ms；跨 JVM 比较含环境波动。输出在 `/tmp/drill-plan-cache-source-liststatus-profile.log` 和 `/tmp/drill-plan-cache-light-check-final-bench.log`。原来直接从本次 `SchemaPlus` 重新查表会触发每个查询首次展开 DFS schema，命中规划约 49–53 ms；该尝试已撤回。

## `SqlBoundDynamicParam` 改造后重测

改造后的链路为 `SqlBoundDynamicParam → RexBoundDynamicParam → 带参数标记的 Drill LiteralExpression`。Paimon 先按“开→关”再按“关→开”运行两轮；Parquet 运行一轮“开→关”。每行均是独立 JVM 和 20 次后续查询；P90 与中位数均以毫秒计。

| 工作负载 | 轮次 | 缓存 | 首次规划 | 后续规划中位数 / P90 | 端到端中位数 / P90 | 后续命中数 |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Paimon `WHERE id = ?` | 1 | 开 | 113 | 59 / 72 | 102 / 128 | 20/20 |
| Paimon `WHERE id = ?` | 1 | 关 | 119 | 97 / 119 | 130 / 165 | 0/20 |
| Paimon `WHERE id = ?` | 2 | 关 | 122 | 85 / 114 | 117 / 153 | 0/20 |
| Paimon `WHERE id = ?` | 2 | 开 | 124 | 56 / 71 | 103 / 134 | 20/20 |
| Parquet `WHERE n_nationkey = ?` | 1 | 开 | 83 | 68 / 85 | 114 / 149 | 0/20 |
| Parquet `WHERE n_nationkey = ?` | 1 | 关 | 69 | 72 / 87 | 114 / 138 | 0/20 |

Paimon 命中后，规划耗时中位数两轮分别降低约 39% 和 34%，端到端中位数分别降低约 22% 和 12%。首次规划的“开/关”差值分别是 -6 ms 和 +2 ms，在这两轮里没有观察到稳定的首次规划额外成本。Parquet 未命中，规划和端到端中位数基本持平；P90 有波动，不能据此认定 Parquet 有性能改善。以上结果仅代表这个本地三行单表负载，跨 JVM 顺序对照也无法排除环境噪声。

## 命中路径分段计时

为定位 Paimon 命中后剩余的规划耗时，临时用 `System.nanoTime()` 对命中路径打点，运行三次相同的 20 次命中基准；测试后已移除打点代码。下表是每次运行后续 20 次查询各阶段的中位数，单位 ms。各列分别取中位数，不能直接相加为整条查询的中位数。

| 运行 | `parser.validate` | 参数绑定总计 | 其中 `readPhysicalPlan`（含 scan 重建） | 其中 Paimon scan 重建 | 其中 splits 生成 | Foreman fragment 准备 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 33.3 | 6.4 | 5.8 | 5.2 | 3.8 | 0.8 |
| 2 | 43.7 | 7.0 | 6.5 | 5.8 | 4.3 | 0.7 |
| 3 | 43.3 | 7.1 | 6.6 | 6.0 | 4.3 | 0.8 |

第三次运行中，`SqlConverter` 构造、SQL 解析、资格判断与生成缓存键的中位数分别约 0.12、0.36、0.15 ms；`PhysicalPlan` JSON 的解析、参数改写、重新序列化分别约 0.12、0.22、0.07 ms。Paimon scan 重建中的表加载约 1.33 ms，`tableScan.plan().splits()` 约 4.32 ms。Paimon 格式目录可读性检查约 0.08 ms；`validate` 内其余耗时仍需更细的 Calcite 校验打点才能归因。三次带打点运行的 Query Profile 规划中位数分别为 45、53、53 ms。临时输出保存在 `/tmp/drill-plan-cache-profile-paimon.log`、`/tmp/drill-plan-cache-profile-paimon-2.log`、`/tmp/drill-plan-cache-profile-paimon-3.log`。

随后单独对 Drill 工作区查表打点：后续 20 次查询的 `WorkspaceSchema.getTable` 中位数约 1.06 ms，其中 `FileSelectionInspector` 约 0.20 ms；该次 Query Profile 规划中位数为 56 ms。因此不能把 `validate` 的约 40 ms 主要归因于 Paimon 目录检查或工作区文件选择。剩余时间位于 Calcite validator 的语义校验内部，尚未细分到具体方法。该次输出在 `/tmp/drill-plan-cache-profile-workspace.log`，临时打点同样已移除。

## 历史结果：同步写缓存

| 工作负载 | 轮次 | 缓存 | 首次规划 | 后续规划中位数 | 端到端中位数 | 后续命中数 |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Paimon `WHERE id = ?` | 1 | 关 | 131 | 119 | 156 | 0/20 |
| Paimon `WHERE id = ?` | 1 | 开 | 141 | 49 | 89 | 20/20 |
| Paimon `WHERE id = ?` | 2 | 关 | 126 | 105 | 145 | 0/20 |
| Paimon `WHERE id = ?` | 2 | 开 | 173 | 58 | 100 | 20/20 |
| Parquet `WHERE n_nationkey = ?` | 1 | 关 | 100 | 73 | 114 | 0/20 |
| Parquet `WHERE n_nationkey = ?` | 1 | 开 | 83 | 85 | 125 | 0/20 |
| Parquet `WHERE n_nationkey = ?` | 2 | 关 | 84 | 76 | 118 | 0/20 |
| Parquet `WHERE n_nationkey = ?` | 2 | 开 | 109 | 80 | 116 | 0/20 |

以上为同步写入版的历史数据：Paimon 命中后的规划耗时中位数在两轮中分别降低约 59% 和 45%，端到端耗时中位数分别降低约 43% 和 31%。首次规划没有收益，第二轮还明显变慢。Parquet 不会命中；开启缓存后规划耗时中位数增加 4–12 ms，符合当时回退路径重复规划的代码行为。

## 历史结果：异步写缓存、`SqlBoundDynamicParam` 改造前

调整为“直接使用已有 `SqlNode` 做资格遍历、首次参数化计划直接执行、查询成功后后台写缓存”后，按相同方法再测一轮。缓存命中仍对原 SQL 做校验，以复核当前表解析和访问资格：

| 工作负载 | 缓存 | 首次规划 | 后续规划中位数 | 端到端中位数 | 后续命中数 |
| --- | --- | ---: | ---: | ---: | ---: |
| Paimon `WHERE id = ?` | 关 | 120 | 101 | 141 | 0/20 |
| Paimon `WHERE id = ?` | 开 | 132 | 52 | 90 | 20/20 |
| Parquet `WHERE n_nationkey = ?` | 关 | 70 | 66 | 103 | 0/20 |
| Parquet `WHERE n_nationkey = ?` | 开 | 78 | 66 | 106 | 0/20 |

新路径中 Paimon 命中规划中位数降低约 49%，端到端降低约 36%；Parquet 规划中位数持平。Paimon 首次规划仍比关闭缓存慢 12 ms；这一轮聚合数据不足以确认是参数化规划本身的成本还是本地测量波动，需补分段计时和重复测试。后台缓存发布为最终一致，基准在首次查询结束后等待该后台任务完成，再采集后续命中样本。

本测试只说明小型单表查询的收益，不代表复杂查询、大表、多 Drillbit 或高并发效果。Paimon 仍会在每次命中时重新加载表并规划 splits；如果这些步骤成为主要成本，收益会缩小。测试过程中还发现 `SELECT name FROM Paimon表 WHERE id = 1` 在缓存开启和关闭时都返回 0 行，需单独排查 Paimon 投影与谓词下推；本次收益测试改用已通过正确性测试的 `SELECT id ... WHERE id = ?`。

复现命令（每次将 `true` 改为 `false` 对照，Parquet 增加 `-Ddrill.plan.cache.benchmark.workload=parquet`）：

```sh
mvn -pl contrib/format-paimon -am \
  -Dtest=PaimonQueriesTest#benchmarkPlanCachePlanningTime \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Ddrill.plan.cache.benchmark.enabled=true \
  package
```
