# Apache Drill Plan Cache 设计草案

状态：设计与首版原型同步更新。本文基于本仓库 `master` 的提交 `425046398`。

当前实现由 `planner.enable_plan_cache` 控制，默认关闭。已完成 Drillbit 级有界内存缓存、跨连接键、`SqlBoundDynamicParam → RexBoundDynamicParam → 带标记字面量`、表达式字符串中的槽位 JSON 往返、缓存命中时遍历物理计划 JSON 中的表达式并替换参数，以及失败回退。原始 `SqlNode` 由正常解析链路产生；查询形态不再限定于单表普通 `SELECT`，排序、聚合、窗口、`LIMIT`、联结、子查询、CTE 和集合查询均可进入候选。会改变算子结构或不会保留为物理表达式的常量保留原值并进入模板键；没有可参数化常量的查询也可按完整模板复用。安全准入仍排除动态函数和已有动态参数。首次查询正常执行，只有最终成功才向有界后台队列提交序列化、读回校验和写缓存任务。含 `GroupScan` 的计划要求每个扫描算子显式声明可缓存；目前 Paimon 和 Iceberg format plugin 已启用，其余 storage / format plugin 和 GroupScan 默认关闭。每个槽位都必须保留在物理表达式中。缓存项记录 storage plugin 配置指纹和每个受支持基表的物理标识到表定义兼容版本的映射；命中时先比较插件配置，再由插件按路径直接读取当前版本，然后从新谓词重新规划 splits。仅数据 snapshot 变化不使计划失效。Parquet 的规划阶段谓词下推可能按首次参数值裁剪 row group，因此不会缓存。随机/动态函数回退正常规划；DATE 保持在模板键中，DECIMAL 通过统一的带类型参数槽跨值复用；固定 NULL 可留在模板键中；`OTHER_FUNCTION` 本身不再触发排除。

## 1. 目标与范围

当前参数值链路为 `SqlBoundDynamicParam(index, SqlLiteral)` → `RexBoundDynamicParam(index, RexLiteral)` → 带 `dynamicParamIndex` 的 Drill 字面量表达式。值由节点自身携带，不再通过 `SqlConverter` 的可变下标列表查找。`LiteralExpression` 是字面量的统一基类并持有参数下标；普通表达式不再携带该字段。

目标是减少重复 SQL 的规划耗时。同一 Drillbit 上的不同连接，只要查询语义所依赖的环境相同，就可复用缓存；连接 ID 不参与缓存键。

主要目标是**模式复用**：先判断 SQL 是否具备缓存资格，再将允许变化的字面量参数化；首次规划出带“参数下标 + 本次值”的完整 `PhysicalPlan`，用 Drill 标准 JSON 序列化并缓存。下次同模板 SQL 命中后反序列化计划，遍历并改写所有物理算子中的表达式。`WHERE id = 1` 和 `WHERE id = 2` 是候选；是否真正可复用取决于扫描插件、优化规则和计划形状。完全相同 SQL 的精确缓存可先作为底层快照能力的验证阶段，不取代模式复用。

首版限定单个 Drillbit 的内存缓存和只读 `SELECT`。跨 Drillbit 的分布式缓存不在首版范围内。`EXPLAIN`、DDL、DML、`ANALYZE`、查询级副作用、临时表、不确定性函数（包括随机函数）以及无法证明依赖可校验的查询默认不缓存。未命中或不满足资格时执行现有规划流程，查询语义不变。

## 2. 现有查询链路

```text
Foreman.runSQL
  -> DrillSqlWorker.getPlan / getQueryPlan
     -> SqlConverter.parse + 自动 LIMIT
     -> SQL handler
        -> validateAndConvert
        -> convertToDrel
        -> convertToPrel
        -> convertToPop
        -> convertToPlan (PhysicalPlan)
  -> Foreman.runPhysicalPlan
     -> QueryResourceManager
     -> MakeFragmentsVisitor + Parallelizer.generateWorkUnit
     -> fragment 启动
```

源码入口：

- `exec/java-exec/src/main/java/org/apache/drill/exec/work/foreman/Foreman.java`：`runSQL`、`runPhysicalPlan`、`getQueryWorkUnit`。
- `exec/java-exec/src/main/java/org/apache/drill/exec/planner/sql/DrillSqlWorker.java`：解析、自动 `LIMIT`、handler 分流与失败重试。
- `exec/java-exec/src/main/java/org/apache/drill/exec/planner/sql/handlers/DefaultSqlHandler.java`：从 SQL 到 `PhysicalPlan`。
- `exec/java-exec/src/main/java/org/apache/drill/exec/physical/PhysicalPlan.java`、`exec/java-exec/src/main/java/org/apache/drill/exec/planner/PhysicalPlanReader.java`：JSON 写入与读回。

当前 prepared statement 在创建时用 `SELECT * FROM (<SQL>) LIMIT 0` 获取列元数据，句柄内保存原始 SQL；执行时仍走 `runSQL` 并重新规划。新缓存可覆盖该执行路径，不依赖 prepared statement 句柄。

## 3. 总体结构

`DrillbitContext` 持有一个线程安全、容量有界的 `PlanCache`，生命周期随 Drillbit。缓存对象不得引用 `QueryContext`、`UserSession`、allocator、Calcite planner/cluster、打开的文件系统句柄或正在执行的 `PhysicalOperator` 实例。

```text
SQL 请求
  -> 解析、自动 LIMIT、校验/函数解析、资格预检查
      -> 不合格：原 SQL 走现有规划流程
      -> 合格：提取字面量 -> 参数化 AST + 本次参数值
          -> 生成模板 key -> PlanCache.lookup
              -> 命中：按物理表标识直接读取并比较版本 -> JSON 反序列化出独立 PhysicalPlan
                        -> 遍历全部算子中的表达式 -> 按下标改写参数字面量
              -> 未命中：携带本次值与参数下标进行现有物理规划
                   -> 合格：PhysicalPlan 序列化为 JSON 并缓存
                   -> 不合格：原 SQL 走现有规划流程
  -> 现有 Foreman.runPhysicalPlan
```

资格判断分两道：**参数化前的预检查**排除随机/动态函数、非 `SELECT` 和已知无法改写的语法；**规划后的最终检查**确认参数下标仍保留在全部需要改写的物理表达式中，且没有不可重建的取值依赖。首次规划会完成 SQL 校验；命中时不重复执行 Calcite 的完整语义校验。最终检查失败时使用原 SQL 的正常规划结果，不缓存该计划。

缓存命中只跳过已缓存的规划工作。每次查询仍新建 `QueryContext`，执行资源规划、fragment 生成和启动。`queryId`、session、当前在线节点和 fragment 分配都由本次查询提供。

接入实现上，`DrillSqlWorker` 使用正常解析链路已有的 `SqlNode` 做安全资格遍历；首次规划在参数化 AST 上经 `DefaultSqlHandler.validateAndConvert` 校验一次。缓存键隔离用户和默认 schema；有效选项指纹保存在缓存项中，命中后做 context 校验。首次规划前递归解析 SQL 中的每个基表，缓存项记录物理标识（storage plugin、format 配置、表路径）到 Paimon 表定义版本的映射。命中时直接取得插件，按已记录的路径读取当前版本；任一版本不同或读取失败则缓存失效并回退正常规划，不重新构建 DFS schema。不能把 SQL 字符串正则替换当成参数化。

### 3.1 建议接口

```java
interface PlanCache {
  Optional<CachedPlan> lookup(PlanCacheKey key);
  void put(PlanCacheKey key, CachedPlan entry);
  void invalidate(PlanDependency dependency);
}

record CachedPlan(
    byte[] physicalPlanJson,
    String textPlan,
    List<ParameterSlot> parameterSlots,
    List<PlanDependency> dependencies,
    CacheEligibility eligibility,
    int formatVersion) {}
```

这是概念接口，最终 Java 类型按项目现有风格调整。缓存项只保存在内存，不能把包含扫描配置或凭据的计划 JSON 写入日志、指标标签或持久化存储。

## 4. `PhysicalPlan` 的 JSON 缓存与复用

### 4.1 写入

首次查询按**本次参数值**完成正常优化，得到可执行的 `PhysicalPlan`。源于 SQL 参数的字面量表达式同时携带参数下标。规划路径只做扫描算子准入检查并登记候选，不做 JSON 序列化。查询最终成功后，通过单线程、有界队列的后台任务用标准 mapper 序列化计划、校验槽位并读回，再发布不可变缓存项；失败或队列满时放弃本次缓存，不影响查询结果。与参考方案相比，这里将缓存放在 Drillbit 级以满足跨连接共享，缓存键也不能使用连接内的 statement ID。

`PhysicalPlan` 的 `PlanProperties.options` 包含规划后的查询选项，`scannedPluginNames` 用于 profile。缓存命中时不能简单把旧 `PlanProperties` 当作本次状态：必须验证相关选项一致，并为本次查询重建需要按请求更新的属性。不能只修改 JSON 字符串中的某些字段。

### 4.2 命中

1. 比较缓存键，再执行 context 校验：依次比较有效选项指纹、storage plugin 配置指纹和每个物理表标识的当前版本；不一致或无法读取时使缓存项失效。首次规划无法解析为支持缓存的基表的查询不缓存。
2. 复制缓存 JSON，递归遍历其中的全部嵌套字段，对含槽位的表达式字符串解析、按下标改写并做类型核对；共享的原始 JSON 不修改。
3. 核对每个参数下标在改写后仍存在，再用当前 Drillbit 的 `PhysicalPlanReader.readPhysicalPlan` 将改写后的 JSON 反序列化为**新的** `PhysicalPlan` 对象图。后续如需支持非表达式字段派生状态，仍需算子专门的重建逻辑。
4. 执行改写后的完整性校验，设置 `QueryContext` 的语句类型与 `textPlan`，把计划交回 `Foreman.runPhysicalPlan`。执行阶段不共享对象图。当前实现用用户和默认 schema 隔离缓存键；选项、插件配置和表兼容版本保存在缓存项中，其他插件默认不参与缓存。
5. 校验失败或读回失败时删除该项并走原有规划流程；不能把失败掩盖成成功命中。

改写和读回成本必须单独测量。`ParquetGroupScan` 的 JSON 构造过程会解析插件并重建文件系统及元数据提供者，因此计划复用不保证命中足够快。若这一成本占比较高，再为已支持的扫描算子设计显式重建接口；不要用通用浅拷贝。

### 4.3 并发

当前未实现同一 key 的 single-flight：并发 miss 可各自规划，成功查询各自提交后台写入。失败结果不做长期负缓存。逐出时不影响已取得的快照；每个查询读回自己的计划实例。

## 5. 模式复用：资格判断、参数化与绑定

现有 Apache Drill 协议和 JDBC prepared statement 不支持服务端动态参数。因此本设计从普通 SQL 中抽取常量来生成带下标的内部参数；若后续引入真正的 Prepare/Execute 协议，二者可以共用后续规划和改写链路。设计参考 [Plan Cache设计方案](https://yuque.antfin.com/olap/yak/lm9dqu2nfg6io6ho) 的 `RexBoundDynamicParam` 与双层 visitor 思路。

### 5.1 资格预检查

当前实现直接在解析链路已有的 `SqlNode` 上做安全检查，首次缓存规划由 handler 对参数化 AST 校验一次；命中时另行校验原 SQL。下文列出的是进一步扩展 SQL 覆盖面时仍需满足的设计约束。

`SqlConverter.parse` 和自动 `LIMIT` 已产生原始 `SqlNode`。安全检查直接遍历这棵树，遇到不确定性函数、动态函数、查询上下文函数或原有动态参数时提前退出；`SqlKind.OTHER_FUNCTION` 不单独构成排除条件。解析阶段的 Drill 函数可能仍是 Calcite 未解析函数，故额外检查本地函数注册表中同名的全部 overload；存在非确定性实现时不缓存。`now()`、`CURRENT_TIMESTAMP`、`CURRENT_DATE` 等时间函数读取本次查询开始时间，即使其 Drill 注解没有标记为随机，也明确排除；其中无括号的 `CURRENT_TIMESTAMP` 等在解析结果中是 `SqlIdentifier`，需要单独检查。各种只读查询结构均可进入候选，写入和 DDL 不进入缓存。首次查询由 handler 校验参数化 AST，命中时再校验原 SQL。来源收集递归穿过联结、子查询、CTE 和集合查询，记录每个可解析且支持缓存的基表的身份和版本；视图等无法提供该版本的来源仍回退正常规划。函数定义变化的精确失效机制尚待补齐。

结构常量（`VALUES` 行、`GROUP BY`、`ORDER BY`、窗口定义、`LIMIT`/`OFFSET`）不生成参数槽，而是保留在模式键中。含分组或排序的查询也保留投影常量，避免投影表达式与分组键、排序键失去对应关系；这类查询仍可复用 `WHERE`、`HAVING` 和联结条件中的参数。保留原始 AST/SQL 作为回退输入。

### 5.2 模式键

在解析后的 SQL AST 上提取允许变化的字面量，记录位置、SQL 类型、精度/scale、nullable 信息和表达式上下文。模式键使用参数槽化的 AST 摘要，保留表名、列名、函数、运算符、投影、排序等结构。不同参数类型产生不同键；原始参数值不进入键。

### 5.3 规划与绑定

模式缓存采用 **Optimize-Once**：首次未命中携带本次参数值规划，生成带参数下标的字面量表达式；后续命中跳过优化，从 JSON 读回计划并改写其中的参数来源表达式。`RexBoundDynamicParam` 继承 `RexDynamicParam`，现有 Calcite/Drill 优化规则未必把它当 `RexLiteral`，因此要逐项验证优化规则是否能使用 bound literal。若某个规则使用首个值改变文件集、算子拓扑或其他不可改写状态，该计划不能直接供其他值复用；需要禁用这种规则、增加取值 guard，或命中时回退重新规划。

改写范围是**全部物理算子内的全部表达式**，不限 `Filter`：`Project`、`Join`、聚合、交换、排序、扫描谓词及插件算子里的 `LogicalExpression` 都要由 visitor 覆盖。若参数值还影响文件选择、分区裁剪、输出 schema 或其他非表达式派生字段，单纯改写表达式不足以保证结果正确；需要该插件的专门重建逻辑，否则此查询回退正常规划。最终资格判断基于计划和参数来源，不能只靠 SQL 文本匹配。

同一个模式可生成通用计划，但它可能慢于按具体常量优化的计划。仅当命中频率与端到端收益足够高时启用；需要保留禁用开关和回退路径。

### 5.4 参数槽在物理计划中的表示与序列化

这里的“参数槽”是**已绑定的字面量表达式附带的参数下标**，不是未绑定的占位符。字面量同时保留第一次规划时的具体值和 `dynamicParamIndex`；未参数化的字面量下标为 `-1`。缓存项保存每个下标在 JSON 中的出现次数；命中改写时从原表达式读取类型并校验新值。一个参数可对应多个物理表达式位置，改写时应同步更新。

`LiteralExpression` 统一承载 `dynamicParamIndex`，`IntExpression` 等字面量继承它；Drill 现有 `ValueExpressions.ParameterExpression` 是代码生成中的方法参数，不适合此处。现有字面量值字段通常是 `final`（如 `IntExpression.i`），因此推荐重写 visitor **新建字面量/父表达式**，而不是直接修改缓存对象或大范围移除 `final`。

`PhysicalPlan` 继续使用 Drill 的**标准 JSON 计划格式**；需要扩展其中表达式字符串的往返编码：

1. 普通字面量保持既有格式。带参数下标的字面量用专用包装语法，例如 `bound_dynamic_param(0, INT, 99)`，其中 `0` 是下标，`INT` 是 Drill 字面量类型，`99` 是**第一次的真实字面量值**。所有已参数化的类型统一使用这一格式；DECIMAL 的类型写为 `VARDECIMAL(precision, scale)`。DATE 不生成参数槽，仍留在模板键中。未定型的 NULL 保留在模板键中，显式定型后可与其他槽位一起复用。
2. 扩展 `ExpressionStringBuilder` 以及 `ExprLexer.g4` / `ExprParser.g4`，使 `LogicalExpression.Se` 写出包装语法、`LogicalExpression.De` 解析后还原同一字面量及下标。不能依赖普通 Jackson 字段：Drill 把 `LogicalExpression` 整体序列化为字符串。
3. 查询成功后在后台用标准 `PhysicalPlan` JSON 和 `PhysicalPlanReader.readPhysicalPlan` 做往返测试，确认下标、值、类型和出现次数完全一致；失败则不缓存。缓存发布是最终一致的，紧随首次查询的下一次查询可能仍未命中。缓存项外层仅增加 key、依赖、下标清单与版本，不改变 `PhysicalPlan` JSON 的 `head`/`graph` 结构。
4. 命中时先反序列化成独立 `PhysicalPlan`，然后改写表达式树；普通外部物理计划里若出现不受控的参数下标，应拒绝执行或按正常 SQL 路径处理。

这一格式保留现有 `PhysicalPlanReader` 的作用，也保证缓存计划与普通计划使用相同的 JSON 序列化/反序列化主链路。

### 5.5 参数槽生成与命中改写

当前实现用 `SqlBoundDynamicParam` 保存原 `SqlLiteral` 和槽位下标；`SqlConverter.convertDynamicParam` 使用 Calcite 推断的参数类型生成 `RexLiteral`，再构造 `RexBoundDynamicParam`。`DrillOptiq.RexToDrill` 将该 `RexLiteral` 转为具体的 Drill 字面量表达式，并只在该实例上设置下标。缓存命中所需的本次字面量列表仍保存在候选项中，但首次规划不再依靠转换器中的可变侧表取值。

**生成**：在通过安全预检查的 SQL AST 上按固定顺序给可参数化的 `SqlLiteral` 编号，生成携带原值的 `SqlBoundDynamicParam`。`SqlConverter` 使用 Calcite 推断类型构造 `RexBoundDynamicParam(index, RexLiteral)`；`DrillOptiq.RexToDrill` 再生成带相同下标的具体 Drill 字面量表达式。优化器若复制、折叠或下推表达式，必须传播下标；丢失下标、同一下标对应冲突类型，或产生不可改写的派生状态时不缓存。

**命中**：取出本次 AST 提取的 `index -> typed value`，校验数量、类型、NULL 和精度。依赖校验通过后，复制缓存 JSON 并遍历全部嵌套字段；对含参数标记的表达式字符串解析 Drill `LogicalExpression`，按下标替换本次值，核对原值与新值的表达式类型，以及每个下标的出现次数。随后通过 `PhysicalPlanReader` 读回本次独立的算子图。没有参数标记的字段保持不变；若标记丢失或类型不一致，删除缓存项并回退正常规划。这个通用改写只覆盖序列化为表达式字符串的字段；非表达式派生状态仍由算子专门重建。

**非表达式派生状态**：参考文档指出扫描谓词等可能是 `LogicalExpression` 之外的产物。对这类字段建立算子/插件专门的改写接口，例如扫描谓词重建；若不能从本次值可靠重建文件集、分区裁剪结果或其他派生状态，该计划不得缓存。遍历全部表达式是必须条件，但不能单独作为正确性证明。

首次未命中直接执行用本次值规划出的计划，缓存的是它的 JSON；后续命中在 JSON 读回后改写。`textPlan` 保存参数槽化模板并在末尾列出本次字面量值，不展示首次查询的参数值；统计估计仍属于模板计划。

## 6. 缓存键、隔离与失效

### 6.1 缓存键

当前缓存键由参数槽化的 AST、查询用户和默认 schema 路径组成；连接 ID 不参与。后续若增加精确缓存，可使用原始 SQL 文本避免规范化改变语义。当前键的组成包括：

- 查询类型与缓存格式版本；默认 schema/搜索路径及解析配置。
- 当前安全主体和权限上下文。不同连接的同一用户可共享；跨用户默认不共享。不得把 session ID 直接放入键，否则无法跨连接复用。
- 规划规则/插件配置版本及可能影响成本或算子选择的集群能力摘要需要在扩展范围时评估；本轮不处理函数更新引起的缓存失效。

键中不包含 `queryId`、连接 ID 和当前 fragment ID。当前在线节点集合若影响物理计划形状，需要纳入规划环境摘要；执行时的具体节点分配仍由 `Parallelizer` 完成。

### 6.2 依赖与失效

当前缓存项的 context 快照记录有效选项指纹、涉及的 storage plugin 配置指纹，以及 SQL 中每个受支持基表的物理标识到兼容版本的映射。选项指纹遍历 `OptionManager` 的 system 默认值、system 覆盖、session 覆盖和 query 覆盖，按优先级保留有效值；不能只使用 `getOptionList()`。命中后先比较选项，再比较配置指纹，最后按物理标识读取版本；变化或读取失败则失效并重新规划。没有主动失效机制，也不重新构建 DFS schema。缓存键只包含用户、默认 schema 和模板；按本轮范围，函数注册表变化不触发失效。若可变 SQL 别名或 workspace 映射后来将同一表名重定向到另一张表，仅检查旧物理表版本无法发现；当前模型要求这类映射保持稳定或另行触发缓存失效。

当前每个键只保存一份计划。相同模板若在多组有效选项之间交替执行，context 校验会拒绝旧项并以新项替换，保证正确性，但可能降低命中率；如需优化这类负载，可在同一模板下保存多个 context 变体。

文件系统扫描尤其需要注意：旧计划可能固定了文件列表，新增文件会导致结果遗漏。TTL **不能单独**作为正确性保证。若插件不能提供可靠的快照标识或命中校验，就不缓存该扫描计划。首版按插件能力白名单开放，避免将所有 `GroupScan` 一概视为可缓存。

### 6.3 Paimon format plugin 接入

`StoragePlugin`、`FormatPlugin` 和 `GroupScan` 的 `supportPlanCache()` 默认返回 `false`。Paimon 与 Iceberg 是 DFS 下已接入的 format plugin；DFS 下的 Parquet、JSON 等格式仍关闭。最终准入检查逐个检查物理计划中的 `GroupScan`，任何一个不支持即不缓存整个计划。

### 6.4 Iceberg format plugin 接入

普通 Iceberg 表以 DFS 选择路径为标识，以 table UUID 和 current schema ID 为兼容版本。命中时重新加载表并验证版本；数据 snapshot 更新不会单独使计划失效。`IcebergGroupScan` 的 JSON 构造路径在表达式改写后重新创建 `TableScan`，调用 `planTasks()` 并重建 work 与 endpoint affinity。`#` 元数据表和显式 snapshot 选择暂不缓存。

`PaimonGroupScan` 的标准 JSON 包含表路径、完整下推谓词 `condition`、投影列和固定 `maxRecords`，不包含规划得到的 `chunks/splits`。命中时先替换 `condition` 中的槽位，再反序列化为新的 `PaimonGroupScan`。构造过程通过 Paimon `ReadBuilder` 按本次谓词重新调用 `TableScan.plan().splits()`，后续 fragment 分配也使用本次 splits。谓词若不再能翻译为 Paimon `Predicate`，该缓存命中失败并回退正常规划，避免在已消除独立 `Filter` 的计划中漏过滤。

`PaimonFormatPlugin.planCacheTableVersion` 使用 `FileStoreTable.uuid() + schema().id()`。UUID 标识表实例，schema ID 标识该实例内的定义版本。该版本保存在缓存项的每个基表依赖中，不写进 `PaimonGroupScan` 的 JSON。命中时先比较当前版本；普通数据 snapshot 增加不会改变表定义版本，但扫描算子读回时会重新规划 splits，因此能读到最新数据。Paimon 系统表及其他非 `FileStoreTable` 暂不缓存。当前插件按路径加载表，没有 catalog UUID；Paimon 1.3.1 的 `uuid()` 因此由表名与最早 schema 文件的修改时间构成，受底层文件系统时间戳精度限制。首次规划前采集版本，避免把规划期间发生 schema 变化后的新版本错误地绑定到旧计划；若表在命中检查与 fragment 执行之间变化，仍依赖 Paimon 原有读取语义。

## 7. 与现有异常、选项及 profile 的交互

- 保留 `DrillSqlWorker` 的函数注册表同步重试和 metastore 元数据异常重试。只有最终成功的查询才向后台提交缓存写入；失败或取消不缓存。命中验证失败转正常规划。
- 自动 `LIMIT` 会改变 SQL AST 和 query-local 选项，必须在模板键计算和 context 快照捕获之前执行现有逻辑。命中也要恢复 `SqlStatementType.SELECT` 等查询上下文状态。
- `textPlan`、`PlanProperties` 和 profile 中的 scanned plugins 由缓存项或本次计划提供；增加 `plan_cache_status`、lookup/validation/deserialize/bind 耗时及回退原因，便于判断真实收益。
- 不缓存已有的 fragment 或 `QueryWorkUnit`。它们带有本次 `queryId`、session、资源分配和在线节点信息。

### 7.1 命中时的 profile 文本计划

缓存项保存由 `PrelSequencer` 生成的模板文本。`RexBoundDynamicParam` 在 explain 文本中显示为 `?0`、`?1`；Paimon scan 的 `LogicalExpression` 通过 explain 专用格式化器直接显示 `?index`。PhysicalPlan JSON 仍使用 `bound_dynamic_param(index, type, value)` 保存首次字面量，供表达式反序列化和类型校验使用；命中时会用本次值覆盖。首次查询及每次命中都在文本末尾追加单独的 `Parameters: ?0 = value, ?1 = value` 行，使用本次 SQL AST 提取的字面量。Profile 的算子行解析器忽略该参数行。

这份文本仍是首次规划的 `Prel` 模板：`ALL_ATTRIBUTES` 中的 row count 和 cost 可能过时，Paimon 本次重建的 splits 也不会自动反映在其中。若将来需要展示本次 scan 的实际摘要，可从绑定后的 `PhysicalPlan` 按字段白名单生成；不能直接把完整物理计划 JSON 写入 profile，因为其中可能包含插件配置或凭据。

## 8. 实施顺序

| 阶段 | 交付物 | 退出条件 |
| --- | --- | --- |
| 0 | 规划阶段耗时拆分；序列化/读回 PoC；随机函数识别验证 | 确定命中后仍需执行的校验及读回成本 |
| 1 | Drillbit 级有界缓存、依赖校验、single-flight、指标和开关；可用精确 SQL 验证底层快照能力 | 计划对象可安全跨连接复制，元数据变化会失效 |
| 2 | 参数化前资格判断、`RexBoundDynamicParam`、表达式下标 JSON 往返、双层 visitor 改写和最终资格检查 | 白名单同形 SQL 跨连接命中；与关闭缓存的结果和错误语义一致 |
| 3 | 逐步扩展插件和可绑定算子字段 | 文件增删、schema/函数/选项/权限变化后不会执行旧计划 |
| 4 | 根据实测优化读回开销、决定是否扩展跨 Drillbit | 端到端延迟收益稳定，内存与失效率可控 |

## 9. 验证与上线门槛

正确性测试：跨连接并发；不同用户及权限变化；默认 schema/选项变化；表、视图、函数、插件配置与文件增删；不同参数类型、NULL、溢出；`EXPLAIN`/DDL/临时表回退；缓存失效与读回异常。每例都与关闭缓存的结果、错误类别和 profile 语义比较。

性能测试同时记录规划 P50/P95、命中验证、反序列化、绑定、端到端 P50/P95、命中率、逐出率和缓存字节数。以相同负载的关闭缓存组作对照；若读回或依赖校验成本接近原规划成本，该插件不启用缓存。

默认关闭，通过 Drillbit 配置或系统选项逐步开启；模式缓存是主要开关，若保留精确缓存则单独控制。任何无法确认正确性的命中都按 miss 处理。

## 10. 待评审决策

1. Paimon 现已成为首个支持缓存的扫描格式；是否要为其他格式增加同等级的重建与校验能力。
2. 当前缓存键按用户隔离；是否需要扩展为跨用户共享。
3. 首批覆盖哪些算子的表达式与扫描谓词等派生字段，以及哪些取值范围可保证计划正确。
4. 参数化前完成实际函数解析的校验成本是否可接受；若不可接受，需要设计保持正确性的轻量函数解析路径。
