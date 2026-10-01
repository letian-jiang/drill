# Storage plugin 接入 Plan Cache

本文描述当前原型的实际接入点，以及判断查询是否命中的方法。通用 storage plugin 接口已提供；DFS 下的 Paimon 和 Iceberg format plugin 已完成适配。`planner.enable_plan_cache` 默认关闭，可通过系统或会话选项开启。

例如，对当前连接执行 `ALTER SESSION SET planner.enable_plan_cache = true`；如需对整个 Drillbit 开启，可使用 `ALTER SYSTEM SET planner.enable_plan_cache = true`。

## 1. 插件需要保证什么

缓存保存的是带参数槽的 `PhysicalPlan` JSON。命中时按本次字面量改写 JSON 中的 Drill 表达式，再反序列化成新的算子图。因此插件必须保证：

1. **可识别表身份与兼容版本。** 首次规划能提取稳定的物理表标识和非空版本；命中时可不构建完整 schema，按该标识读取当前版本。缓存项还保存 storage plugin 配置的 SHA-256 指纹；命中时先比较当前插件配置，再检查表版本。版本应覆盖会使旧计划不兼容的表身份和定义变化。Paimon 使用表 UUID + schema ID。配置或版本变化、读取失败、表消失时应回退正常规划。
2. **参数值不能固化在其他计划字段里。** 参数可能出现在任意算子的表达式，不限于 `Filter`。若参数还影响分区、文件、row group、splits、远端查询、输出 schema、limit 等非表达式状态，扫描算子必须从本次已改写表达式重建这些状态；做不到就不要声明支持缓存。
3. **JSON 往返安全。** 插件的 `GroupScan` 需要能由标准 `PhysicalPlanReader` 序列化、反序列化。JSON 应保留重建所需的表标识、投影、谓词和其他稳定配置，不应把首次查询的临时执行资源、旧 splits 或旧 endpoint 分配当成当前值。
4. **失败必须可回退。** 当前版本读取失败、谓词无法翻译、扫描重建失败或 JSON 读回失败时，缓存命中会失效并回到普通规划，不能执行旧 scan spec。
5. **explain 不显示内部标记。** 若 scan digest 包含参数化的 `LogicalExpression`，覆盖 `GroupScan.getExplainDigest()`，使用 `ExpressionStringBuilder.toExplainString()` 输出 `?index`；JSON 序列化仍使用普通表达式格式。

### DFS 下的 format plugin

- 在 `FormatPlugin` 实现 `supportPlanCache()`，并实现两个 `planCacheTableVersion(...)` 重载：`FileSelection` 用于首次规划采集，`Path` 用于命中时直接读取当前版本。元数据表或不支持的表返回 `null`。
- 在相应 `GroupScan` 实现 `supportPlanCache()`。`PlanCache.canCache` 要求物理计划中的**每个** `GroupScan` 都支持缓存，任意一个返回 `false` 就不发布该计划。
- 在扫描算子的 JSON 构造路径中，用新谓词重新生成取值相关的 scan spec。Paimon 读回时重新调用 `TableScan.plan().splits()`；Iceberg 读回时重新创建 `TableScan` 并执行 `planTasks()`。
- `FileSystemPlugin` 已将通用表身份和版本接口委托给具体 `FormatPlugin`；只有声明支持的 format 才会缓存。

参考实现：[FormatPlugin](exec/java-exec/src/main/java/org/apache/drill/exec/store/dfs/FormatPlugin.java)、[PaimonFormatPlugin](contrib/format-paimon/src/main/java/org/apache/drill/exec/store/paimon/format/PaimonFormatPlugin.java)、[PaimonGroupScan](contrib/format-paimon/src/main/java/org/apache/drill/exec/store/paimon/PaimonGroupScan.java)、[IcebergFormatPlugin](contrib/format-iceberg/src/main/java/org/apache/drill/exec/store/iceberg/format/IcebergFormatPlugin.java)、[IcebergGroupScan](contrib/format-iceberg/src/main/java/org/apache/drill/exec/store/iceberg/IcebergGroupScan.java)。Iceberg 使用 table UUID + current schema ID；数据 snapshot 变化后重新规划任务，`#` 元数据表及显式 snapshot 选择保持关闭。

### 非 DFS 的 storage plugin

非 DFS 插件可直接实现 [StoragePlugin](exec/java-exec/src/main/java/org/apache/drill/exec/store/StoragePlugin.java) 的三个方法：

1. `supportPlanCache()` 返回 `true`，默认为 `false`。
2. `planCacheTable(selection)` 从插件自身的 `DrillTableSelection` 生成 [PlanCacheTable](exec/java-exec/src/main/java/org/apache/drill/exec/store/PlanCacheTable.java)：稳定、不透明的表标识和兼容版本。不支持的表返回 `null`。
3. `planCacheTableVersion(identifier)` 在缓存命中时按保存的标识快速读取当前版本；表消失或不支持时返回 `null`。

`ContextSnapshot` 对所有 `DrillTable` 统一调用这组方法，不依赖 DFS 类型。物理计划中每个 `GroupScan` 还须声明支持缓存，并保证参数改写后的 JSON 反序列化可重建本次 scan spec。仅开启 `supportPlanCache()` 不足以保证正确性。

### 最小正确性测试

- 同一模板的不同字面量、跨连接执行，结果正确且能命中；不支持的扫描混入 JOIN 时整个计划不缓存。
- 数据 snapshot 或文件列表变化但 schema 兼容时，命中仍读到新数据。
- schema 变化、同路径重建、表删除或版本读取失败时，旧缓存不命中。
- storage plugin 或 format 配置变化但表版本不变时，旧缓存不命中。
- JOIN、CTE、子查询中的每张基表都被校验；任一表版本变化即重新规划。
- 比较关闭缓存、首次规划、缓存命中的完整结果；测试谓词下推、投影、聚合、排序及 JSON 读回。

## 2. 如何确认一次查询命中了缓存

查询 profile 页面 **Overview → Plan Cache Hit** 显示 `Yes` 或 `No`；旧版本 profile 缺少该字段时显示 `Unknown`。profile JSON 的 `planCacheHit` 是对应的布尔字段。它只在插件配置、表版本校验及参数绑定成功后设为 `true`。text plan 中的 `?0` 和 `Parameters:` 在首次规划与命中时都会出现，不能单独作为命中证明。

在单 Drillbit、没有并发查询的测试中，可按 [DrillSqlWorker](exec/java-exec/src/main/java/org/apache/drill/exec/planner/sql/DrillSqlWorker.java) 的判定点读取全局计数器：

```java
PlanCache cache = cluster.drillbit().getContext().getPlanCache();
first.queryBuilder().sql(firstSql).run();
cache.awaitWrites(); // 等首次成功执行后的异步写入完成；仅测试夹具使用
long before = cache.getHitCount();
second.queryBuilder().sql(sameTemplateWithNewLiteral).run();
assertEquals(before + 1, cache.getHitCount());
```

计数器只在版本校验通过、参数绑定成功并返回新 `PhysicalPlan` 后递增。它是 **Drillbit 级累计值**，多查询并发时不能由其差值归属单条查询；`awaitWrites()` 是用于测试的同步手段，不是用户 SQL 命令。上述方法亦见 [PaimonYcsbPlanCacheBenchmarkTest](contrib/format-paimon/src/test/java/org/apache/drill/exec/store/paimon/PaimonYcsbPlanCacheBenchmarkTest.java) 的逐查询计数。

当前字段只区分是否命中；`MISS`、`INELIGIBLE`、`INVALIDATED` 和 `BIND_FAILED` 尚未进一步拆分。

## 3. 当前模型的边界

缓存条目按首次解析出的物理表标识重新读取版本。若 SQL 别名或 workspace 映射后来把同一 SQL 表名改指向另一张表，但旧物理表仍存在且版本未变，仅检查旧表版本无法发现重定向。接入这类可变映射时，需要让映射变化触发缓存失效，或把这类引用排除在可缓存范围之外。
