# Native Execution 目录与入口

以 [DESIGN](DESIGN.md)为架构依据；默认入口为同构 Java Drillbit 内的 native RPC engine。

```text
native-execution/
  CMakeLists.txt / prepare.py / build.sh
  DESIGN.md / README.md
  java/src/
    main/java/       JNI scan SPI、Java reader bridge、字段/buffer 布局
    test/java/       对应 Java scan/列式数据回归
  src/
    execution/       NativeEngine、进程共享 Velox CPU/I/O pools
    worker/          MinorTaskRegistry、计划装配、状态/取消
    protocol/        原 Drill protobuf/RPC framing、control/data、冻结路由
    plan/            FragmentPlanConverter、ExpressionBinder、DrillSchema
    velox/           Drill JNI Scan/Receiver/Sender PlanNodes/operators、函数适配
    exchange/        SenderBuffer、ReceiverInbox、DrillHash
    columnar/        Drill wire/缓存布局、native vectors、完整字段元数据
    scan/
      ScanReader.h
      jni/           原 Java plugin 的批量 reader 桥接
    jni/             NativeEngine 生命周期 JNI；低层接口供有效 JNI tests
  tests/
    execution/       Task、Sender、native RPC、JNI 及生命周期
    protocol/        protobuf/framing、多在途 RPC、ACK/断连
    plan/            schema、表达式、Join、Window、TopN 和 partition
    scan/            JNI reader 的隔离 Java fixture
    worker/          隔离引擎的协议/原 reader 回归，不是独立部署入口
```

Java scan API、JNI scan bridge 和 `NativeColumnarBridge` 源码位于 `native-execution/java/src/main/java/`；对应测试位于 `java/src/test/java/`。`exec/java-exec/pom.xml` 注册这两个附加源码目录，并继续将类和源码打包到原 `drill-java-exec` artifact；不创建循环依赖的独立 Maven 模块。包名与 JNI 签名保持不变。Java 节点服务、minor serializer 和能力检测仍位于 `exec/java-exec/`。批次复制/重建的 round-trip fixture 为 `NativeColumnarTestSupport`。

- `NativeExecutionService` 由 WorkManager/Drillbit 生命周期持有，绑定已有 JVM/ScanHost，启动 native listeners，并在唯一 endpoint 注册中发布能力。
- `SimpleParallelizer/FragmentEndpointPolicy` 过滤候选节点，保持原成本/affinity 的 minor placement。
- `NativeExecutionRoutes` 冻结 handle → assignment/backend/control/data 地址；`FragmentsRunner/QueryManager` 使用 native tunnel 提交和取消。
- `NativeMinorPlanSerializer` 保留原计划，增加专用或 generic JNI descriptor；C++ converter 绑定固定 schema 和计算。
- `ScanHost/PluginScanReader/ScanContext` 借用原 registry/options/allocator 等 scan 服务，管理 reader；JNI 导入复制到 native owned vector。
- Native→native 同进程走 C++ inbox；跨进程和到完整 Java root 走原 Drill RPC。

生产 Java fragment 构造器不包含 native wrapper 或 Java compute fallback。旧独立 worker 注册和启动、Velox4J Java 类型/编译器、旧私有协议、native scan/provider/SDK 及无效选项已移除。Java scan tests 借用真实 Drillbit 的服务；低层 JNI 和 C++ engine tests 验证真实列值、取消与资源关闭。C++ tests 自行创建测试 JVM，生产 JNI 只绑定宿主 JVM。

TPC-H SQL、计时对比、集群启动和大数据生成工具留在实验工作区，不属于此源码变更。未注册到 CTest、依赖 benchmark classpath 的旧 plugin RPC runner 已移除；通用 plugin 兼容性由 Java `TestNativeGenericPluginCompatibility` 验证。
