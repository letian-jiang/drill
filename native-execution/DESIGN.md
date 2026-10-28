# Apache Drill Native Execution 设计方案（Review 版）

日期：2026-10-07。状态：实验实现，供架构 Review。

**采用同构 Java Drillbit：Java 保留规划、协调和完整 root fragment；每个节点内嵌 C++ NativeEngine，native 查询的所有非 root minor fragment 在 Velox 执行。所有 scan 均通过 JNI 复用原 Java plugin reader。**

本文按当前同构 RPC 实现整理，替代此前分散的设计草案。实验原始数据和日志留在工作区，不作为源码提交内容。章节中的“设计约定”描述要保持的契约，“已验证”描述已有证据；前者不表示已经覆盖 Drill 的全部算子、函数和 plugin。

## 1. 本轮确定的设计边界

| 项目 | 设计约定 |
|---|---|
| 节点形态 | 只有一种 Java 启动的 Drillbit；每个进程可加载同一套 NativeEngine；只有一个节点身份和一条 ZooKeeper 注册记录 |
| Foreman | 保留原 SQL 规划、fragment 划分、minor assignment、状态协调和取消；执行完整 Java root fragment |
| Native 模式 | 全部非 root minor 使用 native 后端；同节点 Foreman 也可以执行 native 非 root minor |
| 计算 | 原 minor plan 在 C++ 转换为 Velox plan；fragment 内的计算、Sender/Receiver 和 driver 调度由 C++ 完成 |
| Scan | Java plugin 负责规划和读取；通用 JNI reader 将原批次导入 native |
| 失败 | 未支持的算子、函数、类型或运行错误明确失败；没有 Java fragment fallback |
| Schema | 允许在 Task 绑定前发现初始 schema；绑定后固定，变化时报错。暂不支持动态 schema |
| 并行 | 每个 minor 对应一个 Velox Task；Task 内拆分 pipelines/drivers，多个 Task 共享进程线程池 |
| 默认线程池 | CPU 池和 Scan/I/O 池分别按进程可用逻辑 CPU 数设置；Linux 下考虑 CPU affinity |
| 当前验证 | 单节点 TPC-H SF1 Iceberg，跳过 Q5；对照 Java 与 native + JNI scan |

普通 Java 查询继续使用原执行路径。native 模式中调用 Java reader 是预先选定的 scan 实现，不是运行失败后的 fragment 后端切换。Plugin 本来具有的数据库查询下推、投影下推等读取行为可以保留。

本轮不展开内存预算、背压体系、spill、安全及部署配置/版本兼容设计；不将它们设为本次 Review 的前置条件。以下只描述执行所需的基本批次所有权、等待、取消和关闭契约。

## 2. 同构节点与职责划分

```mermaid
flowchart TB
  subgraph bit[一个同构 Drillbit 进程]
    subgraph java[Java]
      foreman[Foreman / SQL 与物理规划]
      root[完整 root fragment]
      services[Plugin registry / ScanHost / 原 reader]
      rpc[原 Java Control / Data 服务]
    end
    subgraph native[C++ NativeEngine]
      control[Native Control / Data 服务]
      converter[Minor plan converter]
      tasks[Velox Tasks / Pipelines / Drivers]
      scans[JNI Scan]
      exchange[Drill Sender / Receiver]
    end
    foreman -->|初始化 RPC| control
    control --> converter --> tasks
    scans --> tasks --> exchange
    scans -->|JNI 读取| services
    exchange -->|Drill Data RPC| rpc --> root
  end
```

这里的 Foreman 是查询角色，native 是 fragment 执行后端，不形成第二种节点。一个节点可以同时是某查询的 Foreman 和 native worker；单节点场景下，root 在 Java，全部非 root 在本进程 NativeEngine。

Java storage plugin 继续拥有 catalog、snapshot、统计、文件/分区枚举、work/split、affinity 和 SubScan 生成逻辑。JNI reader 消费已分配给当前 minor 的原 SubScan，不重新做全表规划或跨 minor 分配。

Java `NativeExecutionService` 负责加载引擎、绑定当前 JVM/ScanHost、启动 native listeners 和关闭。JNI scan 借用本节点原有 plugin 服务；不启动第二个 JVM，也不在 reader 旁创建完整 Java Drillbit 或 Java FragmentExecutor。

**主要入口：** [NativeExecutionService](../exec/java-exec/src/main/java/org/apache/drill/exec/nativeexecution/NativeExecutionService.java)、[NativeEngine](src/execution/NativeEngine.h)。

## 3. 节点发现、调度与查询生命周期

### 3.1 一次注册、两套执行入口

Java Drillbit 继续管理原 ZooKeeper session 和服务记录。原 endpoint 的 Java control/data 地址保持原用途；native 能力发布在同一个 endpoint 的扩展字段内，包含 native control/data 地址。C++ 不另注册一个 worker 身份。

启动顺序是准备 Java plugin 服务、创建 NativeEngine、绑定 native listeners、取得最终 endpoint，再发布可用能力。只有 native listener 和引擎就绪的节点才能进入 native 候选集；服务故障使相关查询失败并撤销可用能力。

`PlanFragment.assignment` 始终保留原规范 Drillbit endpoint。建立 native 连接时使用扩展地址，不通过改写 assignment 伪造一个节点。

### 3.2 冻结 fragment 路由

原并行化器完成 minor assignment 后，Foreman 冻结如下路由，并随初始化消息发送：

```text
fragment handle（queryId / majorId / minorId）
  → 原 assigned Drillbit
  → 执行后端：root = JAVA；所有 non-root = NATIVE
  → 已解析的 control/data 地址
```

Native 能力只决定候选节点和传输入口，不修改原 collectors、destinations、minor ID、hash 桶或硬 affinity 约束。数据发送不逐批访问 ZooKeeper。

现有 `foreman root-only` 调度策略可以将非 root 分配到其他节点；单节点验证使用允许 Foreman 承载非 root 的策略。Root-only 策略若没有可用其他 worker 应明确失败。

### 3.3 调度策略：候选节点、minor 宽度与工作分配

调度保留原 Drill 并行化器。Native 集成改变执行候选集及后端路由，不另建一套集群调度器，也不把 JNI reader 的 work 队列用于跨节点重新分配 minor。

**节点选择：**

| 查询与策略 | Root 候选 | 非 root 候选 | 无可用 worker / affinity 冲突 |
|---|---|---|---|
| 普通 Java 查询 | Foreman | 原在线节点集合 | 沿用原 Drill 规则 |
| Native，Foreman 可参与 | Foreman，Java 后端 | 在线且 native RPC 就绪的同构节点，包含满足条件的 Foreman | 无候选或必需 affinity 指向候选集外节点，失败 |
| Native，Foreman root-only | Foreman，Java 后端 | 上述集合排除 Foreman | 必须有其他 native 节点；不把任务转回 Foreman 或 Java |
| 只有 root 的查询 | Foreman | 不需要 | 不要求存在 native worker |

当前单节点测试使用 Foreman 可参与策略：一个节点承载完整 Java root 和全部 native 非 root。多节点时 root-only 可以隔离 Foreman 的计算负载，但不自动创造 worker，也不能绕过 plugin 的硬 affinity。

Native 就绪检查确认 control/data 服务能力。Plugin 继续由所属 Java Drillbit 的原 registry 解析；算子和表达式支持性在 native 绑定时核对。

**Minor 宽度：**

Soft-affinity fragment 继续以原 `maxOperatorCost / sliceTarget` 计算初始宽度，并按原实现顺序约束：

```text
width = ceil(fragment.maxOperatorCost / sliceTarget)
width = min(width, fragment.maxWidth, query.maxGlobalWidth)
width = min(width, maxWidthPerNode × candidateNodeCount)
width = max(width, fragment.minWidth)
width = min(width, fragment.maxWidth)
width = max(width, 1)
```

这是当前 soft-affinity 代码的实际顺序；operator 最小宽度在后面生效，不将它改写成所有上下限都严格成立的简单 clamp。Root 按原 root 约束留在 Foreman。Hard-affinity fragment 使用原必需节点集合、各节点宽度上限及 affinity 分配逻辑，不套用 soft-affinity 的随机轮转。

当前沿用 Java 规划成本模型，尚未按 Velox 实测成本重新估算宽度。历史性能测试的 planner max width 1 限制的是 major 的 minor 数量，不会把单个 Velox Task 的 driver 上限限制为 1。

**Placement 与 scan work：**

- Soft affinity 在 native 候选集中优先分配有 affinity 的节点，剩余槽位沿用原随机起点轮转；不按 native Task 的实时队列长度重新 placement。
- Hard affinity 必须满足原必需节点和 work 宽度约束。排除 Foreman 或未就绪 native 节点导致冲突时直接失败，不静默降低 affinity。
- Endpoint assignment 后，由原 plugin 给各 minor 生成 SubScan/assigned work。JNI reader 只消费当前 minor 的工作，不能重复枚举或跨 minor 领取。
- 一次查询冻结 backend/地址和 destinations；运行中不迁移 minor、不跨节点 work stealing，也不在失败后重试为 Java fragment。

### 3.4 提交与执行顺序

1. Java Foreman 完成物理规划、minor assignment 和路由冻结，先注册完整 Java root 的接收入口。
2. 按原 Drill 的 intermediate/leaf 启动流程，将原 minor fragment plan 和路由上下文通过 native Control RPC 提交。
3. Native runtime 核对 handle、assignment、后端及目的地址，登记 minor 和预期 Receiver inbox，再确认接纳。
4. Scan 或上游 Receiver 提供初始 schema；C++ 完成表达式绑定、schema 检查和计划转换，创建 Velox Task。
5. Task 在公共 CPU 池中推进 drivers；scan 等待由公共 I/O 池处理，exchange 按冻结路由发送。
6. Native fragment 状态通过原 Control RPC 报告给 Java Foreman；最终输出通过原 Data RPC 进入完整 Java root。
7. 错误或取消终止 Task，通知相关 sender/receiver，关闭 reader；节点关闭先停止接纳并排空 native 工作，再释放 Java scan 服务。

**同进程提交、状态上报和到 Java root 的输出，目前也使用 RPC。** 当前默认路径没有逐查询的 JNI fragment submit、逐批 JNI root callback 或 JNI 状态轮询。生命周期的故障通知 JNI 不属于逐 fragment 状态上报。

初始化 ACK 表示任务已接纳，不等于 schema 已成功绑定或执行已完成。重复提交、先于创建到达的取消、Receiver 提前结束及迟到消息由 runtime 的任务状态处理；错误不能重建为 Java fragment。

**主要入口：** [SimpleParallelizer](../exec/java-exec/src/main/java/org/apache/drill/exec/planner/fragment/SimpleParallelizer.java)、[SoftAffinityFragmentParallelizer](../exec/java-exec/src/main/java/org/apache/drill/exec/planner/fragment/SoftAffinityFragmentParallelizer.java)、[HardAffinityFragmentParallelizer](../exec/java-exec/src/main/java/org/apache/drill/exec/planner/fragment/HardAffinityFragmentParallelizer.java)、[FragmentEndpointPolicy](../exec/java-exec/src/main/java/org/apache/drill/exec/planner/fragment/FragmentEndpointPolicy.java)、[NativeExecutionRoutes](../exec/java-exec/src/main/java/org/apache/drill/exec/nativeexecution/NativeExecutionRoutes.java)、[FragmentRoutes](src/protocol/FragmentRoutes.h)、[MinorTaskRegistry](src/worker/MinorTaskRegistry.cpp)。

## 4. Minor → Task → Pipeline → Driver

```text
Drill major fragment
  ├─ minor A → Velox Task A → pipelines → drivers
  └─ minor B → Velox Task B → pipelines → drivers
                           ↓
               进程共享 CPU 池与 Scan/I/O 池
```

保留 Drill 的 major/minor 分布式任务模型。一个正常启动的非 root minor 创建一个 Velox Task；创建前取消和正常提前结束单独处理。

C++ converter 将 minor 中的原算子树转换为 Velox plan。Velox 按算子边界拆分 pipelines；Join、Aggregation、Sort、Window 等需要的 local partition/gather 由转换器补充。Task 当前以 `runtime.threads()` 作为 driver 上限启动。

Minor 数量表示 Drill 分配的任务数，driver 数量表示单 Task 内可并发的执行实例数，两者独立。增加 driver 不会自动增加 minor，也不会使单路全局 Sort、Gather 或一个不可拆分的 Java reader 变成并行读取。

| Scan 工作形态 | 并发方式 |
|---|---|
| 原通用 plugin 的不透明单 SubScan | 共享一个串行 reader/source，批次可以分发给多个下游 driver |
| 显式声明独立 work 的 JNI scan | 多个 reader 从共享队列领取 work，各 work 执行一次；每个 reader 自身串行 |

CPU 池与 I/O 池各默认使用可用逻辑 CPU 数，并不意味着两池合计只有该数量的线程。所有 Tasks 复用这两池；每个 Task 的 driver 上限也不代表同时占用对应数量的 CPU 线程。Reader 阻塞工作在 I/O 池中执行，driver 等待 future 并让出 CPU。

初始 schema 发现所打开的首个 reader 要继续消费，不能为了绑定再扫一遍首个 work。跨 driver 的工作队列必须保持消费一次，不能简单复制 descriptor 各扫一遍。

### 4.1 节点内 Task 与 Driver 调度

| 调度层次 | 负责组件 | 当前策略 |
|---|---|---|
| 分布式 minor placement | Java 原并行化器 | 规划时确定，保持到查询结束 |
| Minor 接纳、schema 绑定与状态 | C++ MinorTaskRegistry | 接纳后准备 inbox/source；schema 就绪后创建并启动 Task |
| Pipeline/driver 执行 | Velox Task + 进程 CPU executor | 各 Task 共享 CPU 池，按就绪 driver 推进；阻塞时等待 future |
| Reader 创建、读取及预取 | 公共 Scan/I/O executor | 不在计算 driver 中执行阻塞的 Java reader 操作；JNI source 按已有预取实现推进 |
| Minor 内 split/work 领取 | Scan factory/source 的共享队列 | 可拆分时各 reader 领取一次；不透明 SubScan 使用共享串行 source |

启动顺序保留 **root 接收入口 → intermediate 初始化确认 → leaf 提交**。Native intermediate 登记好 inbox 后可先 ACK，真正 Task 启动还可能等上游 schema；不要求上游完成扫描才接纳下游。

当 receiver 无批次、scan 读取未完成或 sender 等 ACK 时，driver 进入 WAIT，由对应 future 唤醒。Velox 按算子约束决定 pipeline 实际 driver 数；Gather 后阶段通常单 driver，增加线程池大小不会消除这些串行阶段。

当前没有独立的 query 优先级、公平份额或根据运行负载动态调整 driver 数的调度器。每个 Task 的 driver 上限都取进程线程数，因此多个 minor 可以产生多于 CPU 线程数的可运行 driver，共享 executor 队列；不能把它解释成每 Task 独占一个线程池。

例如一个拥有 16 个可用 CPU 的单节点，某 major 分配 2 个 minor：创建 2 个 Task，各 Task 的 driver 上限为 16，但共同使用同一个 16 线程 CPU 池及另一个 16 线程 I/O 池。若 scan 只有一个不可拆 work，每个 Task 的 reader 仍串行；如果各 minor 有多个独立 splits，才可以并行读取。

**主要入口：** [VeloxRuntime](src/execution/VeloxRuntime.cpp)、[JNI Scan](src/scan/jni/JniPluginScan.cpp)。

## 5. 两类 JNI 的职责与数据所有权

### 5.1 Java → C++：节点生命周期 JNI

调用方是 Java Drillbit 的节点服务，目标是本进程 NativeEngine。职责包括创建引擎、绑定已有 JavaVM/ScanHost、取得 endpoint、停止接纳、排空、故障处理和关闭。

生命周期跨越节点启动到退出，不跟随每个 query 创建引擎，也不承担默认查询的 fragment 提交、driver 推进、状态报告或 root 数据输出。

低层 JNI submit/accept/root/status 测试接口保留，用于列式值、取消和 reader 生命周期回归；它们不用于默认查询。旧 Velox4J runtime、Java fallback wrapper、EmbeddedNativeService/root callback 投递服务和独立 C++ worker 启动/注册程序已清理。

### 5.2 C++ → Java：Plugin Scan JNI

调用方是 JNI scan source，目标是原 Java plugin reader。流程如下：

```text
assigned SubScan descriptor
  → ScanHost reserve/open
  → 原 BatchCreator / CloseableRecordBatch
  → 原 reader 产生 Drill 列式批次
  → 一次批次读取调用 + importBatch 回调
  → C++ owned RowVector
  → Velox 后续 native 算子
```

通用 `drill-java-subscan` 路径自动序列化原叶子 SubScan，借用原 plugin registry、options、credentials、allocator 和所需 scan 服务。它统一包装 legacy/managed scan，不要求已有 plugin 实现新的 JNI 接口。Metadata reader 所需 schema 树也由公共 ScanContext 按需提供。

桥接采用批量 buffer 地址/长度数组和缓存布局，避免逐行 Java 对象、逐字段 JNI 调用及每批重复序列化 schema。一次 read 内部会回调 native import；不能把它描述成只有一次跨语言切换。

**当前 JNI scan import 会解码/复制到 C++ 持有的向量。** 原 reader 可以在下一次 read 复用 DrillBuf，因此 buffer 地址只在当前导入调用期间有效。当前实现不承诺零拷贝，也不让 Velox 在 Java reader 已复用缓冲区后继续引用其内存。

Native I/O 线程按需 attach 到现有 JVM，复用线程 attachment，在线程退出时释放；`JNIEnv` 只在所属线程使用。Reader 句柄、class/global reference 和异步任务分别遵守所属生命周期，关闭不能与仍使用其资源的导入任务竞争。

JNI source 当前最多预取两批；单 reader 不并发 `next()`。取消可通知正在执行的 reader；未使用的 schema seed、失败的 open 和正常 EOF 都要释放。节点服务先关闭引擎及在途读取，再释放借用的 ScanHost 服务。

| 路径 | Java 工作 | 批次数据跨 JNI |
|---|---|---|
| JNI scan | SQL/plugin 规划、节点生命周期、原 plugin reader 读取 | 有，按批导入 |
| Native 计算 / native exchange | Java 协调，root 仍在 Java | 默认计算及 exchange 不经 JNI；root 输出走 RPC |

**主要入口：** [NativeEngineJni](src/jni/NativeEngineJni.cpp)、[ScanHost](java/src/main/java/org/apache/drill/exec/nativeexecution/scan/ScanHost.java)、[PluginScanReader](java/src/main/java/org/apache/drill/exec/nativeexecution/scan/PluginScanReader.java)（包含通用 reader）、[ScanContext](java/src/main/java/org/apache/drill/exec/nativeexecution/scan/ScanContext.java)。

## 6. Fragment 之间的通信

| 发送方 → 接收方 | 同一进程 | 不同进程 |
|---|---|---|
| Native 非 root → Native 非 root | C++ Sender → ReceiverInbox，传递 native 向量 | C++ BitData RPC，Drill wire schema 与 buffers |
| Native 非 root → Java root | 回环连接到原 Java BitData 服务 | 连接 Foreman 的原 Java BitData 服务 |
| Java → Java（普通 Java 查询） | 原 Drill 本地路径 | 原 Drill RPC |
| Java 非 root → Native 非 root | 当前 native 模式无此混合计算边 | 当前 native 模式无此混合计算边 |

JNI scan 是同一个 native fragment 内的 source，不是额外的 Java fragment，所以不增加 Java/native fragment exchange 边。

同进程 native exchange 保留原 sender/receiver handle、schema、结束和取消语义，但避免 wire 数据编码。向量可在满足所有权条件时接管，其他情况复制；receiver 按需转移到自身 pool。不能将整个本地路径统称为无条件零拷贝。

跨进程与到 Java root 的路径使用原 Drill 协议消息结构：query/major/minor 标识、RecordBatchDef、SerializedField、数据 buffers、ACK、EOS、receiver-finished。Native Sender 使用 Drill partition hash 和原 destinations；不能替换为 Velox 自己的跨节点 hash 分区协议。

Receiver 聚合原 collector 规定的上游 sender；schema announcement 与 EOS 区分，即使零数据也需要处理初始字段。Unordered Receiver 导入后提供 native source；Merging Receiver 恢复有序输入语义。发送缓冲区保留到消费/ACK 完成，WAIT 唤醒推进 driver。

Native fragment 状态回原 Java control；Receiver 提前结束通知对应后端的 sender，防止继续向已关闭的下游发送。

目前只做单节点验收。跨进程通路属于已实现的架构组成，不把本轮单节点结果解释成当前版本的多节点性能或全面协议兼容证明。

**主要入口：** [RpcTransport](src/protocol/RpcTransport.cpp)、[ReceiverInbox](src/exchange/ReceiverInbox.cpp)、[SenderBuffer](src/exchange/SenderBuffer.cpp)、[DrillHash](src/exchange/DrillHash.cpp)。

## 7. 原 Minor Plan → Velox Plan

Foreman 保留原 Drill 物理计划。Serializer 只补充 scan descriptor 和执行所需信息；C++ 读取原 minor plan、绑定表达式并完成 lowering。原 operator ID 用于关联执行统计，新增辅助 Velox 节点使用独立 ID。

| 原 Drill 算子 | 当前 native 转换方式 | 主要约束 |
|---|---|---|
| Scan | JNI BatchSource + 原 Java plugin reader | 原 reader 服务依赖、固定 schema、类型及 buffer 布局必须支持 |
| Unordered/Merging Receiver | DrillReceiver；有序路径补排序/汇聚 | 按原 collectors 和 sender 完成条件 |
| Project / Filter | Velox Project / Filter | 原函数及字段类型必须能够绑定；通配符在 schema 绑定时展开 |
| Hash/Streaming Aggregate | Velox Aggregation，必要的 local partition/gather | 保留原 fragment 的聚合阶段；转换 `$sum0`、single_value 等 Drill 语义 |
| Hash/Merge Join | Velox HashJoin | 依据原 join 类型、条件、输出 schema；MergeJoin 不强制保留 merge 算法 |
| Nested Loop Join | Velox NestedLoopJoin | 条件和类型可转换 |
| Sort / External Sort | Velox OrderBy + 必要 Gather | 本轮不提供 Drill spill；reverse sort 未支持 |
| TopN / Limit | 局部 TopN + Gather + 最终 TopN；Gather/Limit | 保持 minor 内全局结果，不给每个 driver 独立应用最终 limit |
| Union All | 输入列对齐/转换 + LocalPartition Gather | 已覆盖固定兼容类型，不承诺异构 union schema 的完整 Drill 语义 |
| Window | 分区/汇聚 + Velox Window | 有限 offset frame 尚未支持 |
| Flatten | Unnest + Project | 已支持类型和 NULL 规则范围内 |
| Selection Vector Remover | 消去，数据已用 native 向量表示 | source/wire 的 selection vector 要先规范化 |
| Single / Hash Partition / Broadcast Sender | DrillSender | 原 destinations；hash 规则与 Drill 一致 |
| Ordered Partition Sender | 尚未形成可用执行支持 | converter 识别节点名不等于 runtime 支持 |

该表是转换路径清单，不是所有函数签名、join 条件、嵌套类型及 NULL 组合均已验证的声明。例如前次 plugin 测试发现 `MOD` lowering 未支持；选择 CASE 的 reader 测试不构成 MOD 已修复的证据。

支持判断分层进行：Java 决定 native 后端并补充 JNI descriptor；C++ 检查 operator/JNI reader binding；拿到 source schema 后检查类型和表达式；批次检查 schema/布局。能够在提交前确定的问题应早报，依赖运行时 schema 的问题允许在 Task 绑定时失败。每层都不选择 Java fragment fallback。

Q5 按用户要求跳过，当前验收是其余 21 条；不能据此宣称整个 TPC-H 22 条或所有非 scan Drill 算子已经完整覆盖。

**主要入口：** [FragmentPlanConverter](src/plan/FragmentPlanConverter.cpp)、[ExpressionBinder](src/plan/ExpressionBinder.cpp)、[DrillOperators](src/velox/operators/DrillOperators.cpp)。

## 8. 类型映射、固定 Schema 与 Drill 语义

### 8.1 标量类型映射

下表描述当前通用 JNI/Drill wire 桥接和 native 计算边界。原 plugin reader 的输出遵循 Drill 类型和布局，绑定后的字段树保持固定。

| Drill 类型 | Velox 类型 / 存储 | 导入与输出规则 |
|---|---|---|
| INT | INTEGER / int32 | 保持有符号 32 位值 |
| BIGINT | BIGINT / int64 | 保持有符号 64 位值，不经浮点转换 |
| FLOAT4 | REAL / float32 | 保持值表示；比较/hash 的 Drill 语义单独适配 |
| FLOAT8 | DOUBLE / float64 | 同上；不把类型映射视作全部 NaN/±0 运算语义一致的证明 |
| BIT | BOOLEAN | Drill packed bits 与 Velox Boolean vector 转换 |
| VARCHAR | VARCHAR / StringView | Drill offsets + UTF-8 bytes；保留长度及字段 metadata，不改写字符串内容 |
| VARBINARY | VARBINARY / StringView | 保留任意 bytes，包括 NUL/非 UTF-8；不能作为字符串解码 |
| VARDECIMAL(p,s) | DECIMAL(p,s)，short int64 / long int128 | Drill 有符号大端变长 unscaled integer ↔ native 定宽 unscaled integer；保留 p/s，不转 DOUBLE |
| DATE | DATE / int32 days | Drill int64 epoch 毫秒 ↔ UTC epoch 天数；导入要求午夜整天且天数不溢出 |
| TIME | TIME / int64 毫秒 | Drill int32 毫秒扩展为 native int64；输出检查 int32 范围 |
| TIMESTAMP | TIMESTAMP / seconds + nanos | Drill int64 epoch 毫秒分解成 seconds/nanos，正确规范化负 epoch；输出回毫秒 |

Decimal 计算结果的 precision/scale、溢出和 cast 规则由表达式/聚合适配负责。Temporal 的上述映射不增加时区类型；JNI 导入保留原 Drill reader 的毫秒表示。

### 8.2 复合类型、Mode 与向量编码

| Drill 类型或属性 | Native 表示 | 语义与限制 |
|---|---|---|
| MAP（命名字段结构） | Velox ROW + 原 Drill 字段树 | **不是 Velox MAP**；字段名、顺序、子字段和 mode 保留 |
| DICT（key/value） | Velox MAP(K,V)，由 DrillMapType 保留元数据 | **不是 Velox ROW**；key 为受支持的非 NULL 标量，value 可为已支持的嵌套类型 |
| `T REPEATED` | Velox ARRAY(T)，DrillArrayType 保留原表示 | Drill offsets + 子列；与 LIST 区分输出 mode/布局 |
| 同质 LIST | Velox ARRAY(T)，固定 element schema | LIST 的 offsets、父 validity 和子列转换；异构/晚绑定 element 不支持 |
| REPEATED MAP / 嵌套数组 | ARRAY(ROW(...)) / ARRAY(ARRAY(...)) | 递归转换已支持字段树；保留各层 LIST/REPEATED 表示 |
| REQUIRED 标量 | 无合法 NULL 的 native 列 | 输出拒绝 NULL，并使用 required wire 布局 |
| OPTIONAL 标量/LIST | Native null bitmap | Drill validity 表示与 Velox null bitmap 转换；NULL 与零值/空数组区分 |
| OPTIONAL MAP | 保留 OPTIONAL 声明的 ROW | 原 MapVector 无父 validity；不因此支持计算产生的真实父级 NULL ROW |

DICT 导入不为了套用普通 map 语义而排序或去重；当前 Drill lookup 适配按原 reader 的“最后一个重复 key 生效”规则执行。类型映射不代表所有 Velox map 函数自动符合 Drill DICT 语义。

Drill 的 REQUIRED/OPTIONAL/REPEATED 是字段语义和 wire 布局属性，不对应一套独立 Velox 标量类型；metadata 必须另行保留。透传列保留原属性，计算列按表达式实际 nullability 构造，outer join 只放宽相应一侧。

Velox Flat/Constant/Dictionary 是**向量编码**，不等同于 Drill DICT 类型。计算及本地 exchange 可以使用这些编码，编码回 Drill wire 时通过 decoded vector/选择行规范化；原 Selection Vector 的移除也不修改字段逻辑类型。

### 8.3 未支持类型及支持判定

当前通用列式桥接没有直接支持 TINYINT、SMALLINT、无符号整数、旧固定/稀疏 DECIMAL、INTERVAL、带时区 temporal、异构 UNION 或动态 LIST element。此清单指原字段直接过桥；若原计划或 reader 已合法输出支持类型，按最终字段绑定，不人为禁止对应 SQL 来源。

纯 NULL/UNKNOWN 可以作为表达式绑定过程的中间类型，不是可直接发送的 LATE wire 列；输出必须得到可编码的固定类型。新增类型需要同时实现 Java 布局、native 导入/导出、schema 元数据、表达式、hash 及 local/RPC exchange，不能只新增一个类型枚举。

支持性按实际字段树递归检查，不支持时明确失败，不自动把 DECIMAL 转 DOUBLE、把未知列转 VARCHAR，也不切换 Java fragment。已支持类型仍要检查具体函数签名和布局；类型可导入不等于所有运算已支持。

**类型实现入口：** [NativeColumnarBridge](java/src/main/java/org/apache/drill/exec/physical/impl/velox/NativeColumnarBridge.java)、[ScanBatchLayout](java/src/main/java/org/apache/drill/exec/nativeexecution/scan/ScanBatchLayout.java)、[ColumnarBatch](src/columnar/ColumnarBatch.cpp)、[DrillArrayType](src/columnar/DrillArrayType.h)、[DrillMapType](src/columnar/DrillMapType.h)、[DrillDict lookup](src/velox/functions/DrillDict.cpp)。

### 8.4 固定 Schema 与输出元数据

Velox RowType 不能单独表达 Drill 的全部字段属性。Native 路径保留 Drill `MaterializedField/MajorType` 元数据，包括 REQUIRED/OPTIONAL/REPEATED、precision/scale、嵌套子字段、其他类型属性和 proto2 presence。

当前通用批次桥接覆盖 INT、BIGINT、FLOAT4、FLOAT8、BIT、VARCHAR、VARBINARY、DATE、TIME、TIMESTAMP、VARDECIMAL，以及已支持叶子类型组成的 MAP、DICT、REPEATED 和同质 LIST 树。类型可导入不等于该类型的全部运算可绑定；新增类型需同时覆盖读取布局、表达式、hash、输出字段和 exchange。时间输入输出均遵循 Drill 毫秒表示。

元数据绑定到各自的 batch/output RowType，不能修改共享 primitive Type。JNI 缓存 layout 与普通 columnar type 都必须携带原字段描述。Project、Aggregate、Join、Window、Flatten、Union 转换后根据输出语义传递或构造字段属性；outer join 只将对应侧放宽为 nullable。

Drill wire 输出的字段声明必须与 buffer 布局一致。REQUIRED 列使用 required 布局并拒绝 NULL，不能只改元数据而继续发送 optional validity buffer。数组下标和 DICT key 取值即使输入元素为 REQUIRED，结果也声明为 OPTIONAL，因为越界或缺失 key 会返回 NULL；字段解引用同时传播父字段的 optional 属性。

**固定 schema 是绑定后的约束，不是要求所有 plugin 在规划阶段知道 schema。** 首批/初始 reader 可以发现字段，receiver 也可以从 schema announcement 绑定。绑定后新增列、字段类型或布局变化应明确失败并释放 reader；本轮不做 schema 预扫、运行中重绑、Task 重建或动态 variant 计算。

已验证固定 schema 下选定 plugin 的完整输出类型；空结果 header 尚未纳入严格等价对照，真正的 ROW 父级 NULL 与异构 Union 也不能视为已完整支持。

**主要入口：** [DrillSchema](src/plan/DrillSchema.cpp)、[DrillRowType](src/columnar/DrillRowType.h)、[ColumnarBatch](src/columnar/ColumnarBatch.cpp)、[DrillBatchMetadata](src/columnar/DrillBatchMetadata.cpp)。

## 9. 代码目录与修改边界

Native 代码全部位于 `drill/native-execution`：

```text
drill/native-execution/
  java/src/
    main/java/        Java scan 接口、JNI bridge 和 columnar layout
    test/java/        对应 Java 回归
  src/
    execution/        NativeEngine、公共 Velox runtime
    worker/           MinorTaskRegistry、任务与状态协调
    protocol/         Drill RPC/protobuf、fragment 路由
    plan/             原计划转换、表达式绑定、Drill schema 传播
    velox/            Drill PlanNode/算子、hash/decimal/numeric 等函数适配
    exchange/         Sender buffers、Receiver inbox、Drill hash
    columnar/         Drill buffers ↔ native vectors、字段元数据
    scan/
      ScanReader.h
      jni/            通用 Java reader bridge
    jni/              Java Drillbit → NativeEngine 生命周期接口
  tests/              执行、协议、schema、reader、取消与模块测试
```

Java scan 公共接口、JNI scan bridge 和 columnar layout 源码位于 `native-execution/java/`，由 `exec/java-exec` 作为附加 source roots 编译/打包，保持原包名和 JNI 签名。节点生命周期和调度扩展仍位于 `exec/java-exec/`；serializer/columnar bridge 保留历史 `physical/impl/velox/` 包名，不表示仍采用 Velox4J 或旧宿主方案。

修改原 Drill 的集中边界是节点生命周期与 endpoint 发布、并行化候选/路由、fragment 初始化协议、Java root 的批次接收，以及必要的语义修复。原 plugin 的读取实现不因通用 JNI 接入而改写；可选 JniScanProvider/PluginScanFactory 仅描述原 Java reader 及可独立读取的 work。

旧独立 worker 启动/注册和 Velox4J 执行器已移除；独立 scan 宿主和 plugin context facade 已移除；Java scan 回归借用真实 Drillbit，C++ JNI 回归使用测试专用 JVM。详细文件定位见 [CODE_STRUCTURE](CODE_STRUCTURE.md)，当前职责以本文和所链接的实现为准。

## 10. 验证与性能结果

本轮移除后的构建通过 120 个 Java 测试、20 项 CTest、Checkstyle 和 RAT。单节点 SF1 除 Q5 外的 21 条完整结果与 Java 参考一致；136 个非 root minor 全部 native，所有 scan 均经 JNI。该轮每条执行一次用于正确性与路径回归，不作为重复性能测量。

通用 JNI 路径的回归包括 CSV/JSON/Parquet、JDBC/H2、sys 和 information_schema 原 reader，复杂字段/时间/Decimal 布局、取消与资源关闭，以及完整 Java root 与 native RPC 的生命周期。绑定后动态 schema 明确失败；这些用例不代表全部 plugin 和 SQL 组合均已覆盖。

性能结果来自本次清理前的同构 JNI 引擎，不能直接视为删除后的二进制验收。单节点 TPC-H SF1 Iceberg 的 Q1/Q2/Q6/Q16/Q21，每组相同 16 核、2 次预热、3 次测量：

| 对比 | 五条查询中位数之和 | Java/JNI 比值 |
|---|---:|---:|
| Java minor 上限 4 | 7.711 s | 1.59× |
| Java minor 上限 8 | 7.855 s | 1.62× |
| Java minor 上限 16 | 8.743 s | 1.81× |
| Java 每条从上述三档取较优值 | 6.761 s | 1.40× |
| JNI，每 major 一个 minor、Task 最多 16 driver | 4.840 s | — |

100 次执行结果一致。各组统一 4GB heap/8GB direct memory，并开启原 Java HashJoin 的内存内回退以避免 Q2 分区预算检查失败。该选项仅影响原 Java HashJoin 内存策略。minor 上限不表示每个 major 都有该数量；scan 宽度还受 work 数量约束。

同一当前版本的单 driver 控制实验保留 16 个 CPU/I/O 线程，每条 pipeline 一个 driver，五条合计相对低宽度 Java 为 1.33×，Q6 接近持平。不能将多 driver 相对低宽度 Java 的收益归结为单线程计算提升，也不能把五条的结果推广到全部 21 条查询。

用户性能目标仍为 JNI 每条快于 Java。后续用 Java 充分并行作为对照，单独核对 reader/import、native 计算、exchange 和 root 的耗时；Q5 按约定跳过。实验原始数据和工具留在工作区，不随源码提交。

## 11. 本次 Review 的核心边界

1. NativeEngine、JNI plugin reader 桥接和原 Drill 执行协议接入：完整 root 保留 Java，全部非 root 在 native 执行。
2. 原节点注册发布 native control/data 能力，保持同一 Drillbit 身份。
3. 保留原并行化器的 minor/affinity 模型，增加 native 候选与冻结路由。

计算没有 Java fragment fallback。Scan 统一复用原 Java plugin；本分支不包含 C++ scan provider、SDK reader 或外部 scan 模块加载。独立正确性修复保证 Java/plugin 读取与 native 结果一致。
