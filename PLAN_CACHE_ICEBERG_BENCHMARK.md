# Iceberg plan cache 正确性与性能测试

## 测试范围

基于 Drill 自带的 Iceberg 0.12.1 format plugin，在单 Drillbit、本地文件系统上创建一张 1000 行、8 个 Parquet 数据文件的 Iceberg 表。测试查询为 `SELECT id FROM dfs.tmp.iceberg_cache_main WHERE id = <三位整数>`。先预热同模板查询，再使用两个连接交替执行 20 组成对查询：一个连接启用 `planner.enable_plan_cache`，另一个会话关闭。每组交替调整两种连接的先后顺序。两组查询结果均为 1 行，命中状态由 query profile 的 `planCacheHit` 验证。

规划时间采用 query profile 的 `planEnd - start`；端到端时间采用测试客户端的 `runTimeMs()`，单位均为 ms。下表每行是独立 JVM 运行的 20 个样本中位数。

| 运行 | 命中规划 | 关闭缓存规划 | 命中端到端 | 关闭缓存端到端 |
| --- | ---: | ---: | ---: | ---: |
| 1 | 11 | 82 | 43 | 113 |
| 2 | 11 | 76 | 35 | 104 |
| 3 | 11 | 82 | 42 | 113 |

三轮中位数的中位数：规划 11 ms 对 82 ms，下降约 87%；端到端 42 ms 对 113 ms，下降约 63%。命中仍重新加载 Iceberg 表、根据新谓词执行 `planTasks()` 并计算文件块位置，因此这些数字主要体现跳过 Calcite 等上层规划的收益。此测试只有本地小表和单 Drillbit，不能直接预测大表、远端对象存储或高并发环境的收益。

## 正确性

`IcebergPlanCacheTest` 的 7 项测试通过，覆盖跨连接的整数、字符串和 decimal 参数复用；新谓词使扫描任务为空；数据 snapshot 追加后保持命中并读到新数据；partition spec 变化后仍重建扫描任务；schema 变化后拒绝旧计划；同路径重建后 UUID 改变；`#` 元数据表不缓存。现有 `IcebergQueriesTest` 的 28 项测试也全部通过。

数值字面量的精度是当前模板键的一部分，因此性能样本和预热查询均使用三位整数；不同精度的字面量不会误算为同一模板的命中。显式 snapshot 选择暂不参与缓存。

复现命令：

```sh
mvn -pl contrib/format-iceberg -am package \
  -Dtest=IcebergPlanCacheTest,IcebergQueriesTest \
  -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false -DskipITs
```
