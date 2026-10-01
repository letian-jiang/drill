# Paimon YCSB 风格读取测试

当前“表标识 → 版本”直接校验实现的开关对照见 [最新性能报告](PLAN_CACHE_PERFORMANCE_REPORT.md)；下文保留原实现及改动过程中的历史数据。

测试日期：2026-09-29。本测试在本地生成一张 Paimon 主键表 `usertable`：10,000 行，`ycsb_key` 为主键，另有 `field0`–`field9` 十列，每列为 100 字节确定性字符串。表使用单 bucket。数据和查询由 [测试类](contrib/format-paimon/src/test/java/org/apache/drill/exec/store/paimon/PaimonYcsbPlanCacheBenchmarkTest.java) 生成；这是 **YCSB 风格的只读负载**，没有运行 YCSB 客户端，也没有模拟 Zipf 分布或更新操作。

三个 SQL 模板分别执行点查（返回 `field0, field9`）、50 行 key 范围扫描、有序扫描（`ORDER BY ycsb_key LIMIT 10`）。每个模板先运行一次冷查询，等待异步缓存写入，再在两个 Drill 连接间交替执行三个不同 key 版本，共 9 次。每个版本另外核对完整返回值，并计算按行排序后的 SHA-256 指纹。

| 模板 | 关闭缓存规划中位数 | 开启缓存规划中位数 | 规划降幅 | 关闭缓存端到端中位数 | 开启缓存端到端中位数 | 端到端降幅 | 后续命中 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 点查 | 132 ms | 67 ms | 49% | 176 ms | 117 ms | 34% | 9/9 |
| 50 行范围扫描 | 106 ms | 53 ms | 50% | 147 ms | 88 ms | 40% | 9/9 |
| 有序扫描，固定 `LIMIT 10` | 122 ms | 53 ms | 57% | 192 ms | 123 ms | 36% | 9/9 |

关闭缓存组的三个模板均为 0/9 命中。开关两组的 9 个版本逐一具有相同的结果指纹；开启组还通过了按生成规则逐行构造的预期结果断言。`LIMIT 10` 保留在模板 key 中，仅扫描起始 key 参与参数化。

规划时间为查询 profile 的 `planEnd - start`，端到端时间为 `QuerySummary.runTimeMs()`。每组使用独立 JVM、单 Drillbit，顺序运行。表格为单轮 9 次样本的中位数，不代表正式 YCSB 吞吐量或高并发收益。写入和更新性能不在此次范围内。

## 复现

```sh
mvn -pl contrib/format-paimon -am \
  -Dtest=PaimonYcsbPlanCacheBenchmarkTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Ddrill.ycsb.cache.enabled=false \
  -Ddrill.ycsb.samples=9 package

mvn -pl contrib/format-paimon -am \
  -Dtest=PaimonYcsbPlanCacheBenchmarkTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Ddrill.ycsb.cache.enabled=true \
  -Ddrill.ycsb.samples=9 package
```

原始输出：[关闭](artifacts/ycsb-paimon-off.log)、[开启且核对逐行内容](artifacts/ycsb-paimon-on-verified.log)。

## 表版本直接校验

2026-09-29 将命中时的来源校验改为按缓存中的物理表标识直接读取 Paimon 表版本，避免重新解析 DFS schema。使用上述同一测试类、单 Drillbit、开启缓存、每模板 6 次命中样本，改动前后分别在独立 JVM 中顺序运行：

| 模板 | 改动前规划中位数 | 直接校验后规划中位数 | 改动前端到端中位数 | 直接校验后端到端中位数 | 命中 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 点查 | 71 ms | 15 ms | 121 ms | 63 ms | 6/6 |
| 50 行范围扫描 | 63 ms | 14 ms | 122 ms | 54 ms | 6/6 |
| 有序扫描 | 70 ms | 15 ms | 158 ms | 63 ms | 6/6 |

三个模板的返回行数与逐行内容均通过测试断言。对应原始输出：[改动前](artifacts/ycsb-default-schema-baseline.log)、[直接校验后](artifacts/ycsb-table-version-map.log)。这些是本地单轮小样本，不能据此推断并发吞吐量。
