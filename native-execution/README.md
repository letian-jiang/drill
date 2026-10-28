# Drill Native Execution（Experimental）

同构 Java Drillbit 内嵌 C++ NativeEngine：Java 保留 SQL 规划、协调和完整 root；native 查询的所有非 root minor 转换为 Velox Task。Scan 统一通过 JNI 复用原 Java plugin reader，计算不回退到 Java fragment。

[设计方案](DESIGN.md)包含类型映射、minor/driver 调度、两类 JNI 和通信契约；[目录说明](CODE_STRUCTURE.md)定位实现。动态 schema 暂不支持，绑定后变化明确失败。

## Java scan 源码

JNI scan 桥接、scan 接口和字段/buffer 布局位于 `java/src/main/java/`，对应回归位于 `java/src/test/java/`。这些类依赖原 Drill scan/context API，作为 `exec/java-exec` 的附加 source roots 编译，并随原 artifact 打包；包名、JNI 签名和运行时 classpath 不变。节点生命周期、调度和 fragment 提交接入仍位于 `exec/java-exec/`。

## 构建与依赖

当前工具链面向 Linux amd64 / AVX2，C++20、JNI JDK、CMake 3.28+、Ninja，以及 Boost/Folly 开发依赖和 RE2。

```sh
# 在 Drill 仓库根目录。参数为已有 Boost/Folly 开发依赖安装前缀。
native-execution/build.sh /path/to/dependency-prefix
ctest --test-dir .tools/native-runtime/build --output-on-failure -j 2
```

`prepare.py` 下载固定 revision 的头文件和校验 SHA 的公开二进制包，提取匹配的 Velox/Folly C++ 库。不安装 Velox4J Java artifact，不使用其 Java Task API，不加载 libvelox4j 或包内 librt。当前二进制使用旧 libstdc++ string ABI 和 AVX2，尚未提供完整 Velox 源码构建。

默认 cache 是仓库根目录 `.tools`；可以用 `DRILL_NATIVE_TOOLS` 复用已有 cache。`DRILL_NATIVE_BUILD_JOBS` 控制构建并发，`DRILL_NATIVE_DEPENDENCY_PREFIX` 和 `DRILL_NATIVE_CMAKE_PREFIX` 指定开发依赖。生成文件不进入 Git。

```sh
# 构建 Java 和相关 plugin；package 阶段满足原 vector-types unpack 依赖。
mvn -pl exec/java-exec,contrib/format-iceberg,contrib/format-paimon,contrib/storage-jdbc \
  -am package -DskipTests
```

宿主 JVM 设置 `-Ddrill.native.engine.library=/absolute/path/libdrill_native_engine.so` 加载引擎。CPU 与 Scan/I/O 池各默认使用可用逻辑 CPU 数，考虑 CPU affinity；可用 `drill.native.engine.threads` 指定数量。

```sql
ALTER SESSION SET `exec.native_fragment.enabled` = true;
```

`exec.native_fragment.enabled` 启用全部非 root native 执行；不支持的计算明确失败。`exec.foreman.root_only` 控制是否排除 Foreman 的非 root placement，单节点须为 false。`exec.native_scan.batch_records` 设置托管 JNI reader 的默认批次记录数。

## JNI plugin reader

原 SubScan 自动装饰为 `drill-java-subscan` descriptor，由 [PluginScanReader](java/src/main/java/org/apache/drill/exec/nativeexecution/scan/PluginScanReader.java) 使用本节点原 registry、BatchCreator 和 scan 服务打开 reader。原 plugin 无需为通用接入修改代码。批次按 buffer 地址/长度数组导入 C++ owned vectors，绑定后的 schema 必须固定。

可选 [JniScanProvider](java/src/main/java/org/apache/drill/exec/nativeexecution/scan/JniScanProvider.java) 可声明 `independentWorkList`，允许多个 driver 领取互不重叠的 work；未声明时保持一个串行 reader/source。专用 [PluginScanFactory](java/src/main/java/org/apache/drill/exec/nativeexecution/scan/PluginScanFactory.java) 仍复用原 Java reader，适用于保留 Iceberg planned snapshot 和逐 work reader 的优化。Reader 不执行 Java fragment 计算或 exchange。

## 回归验证

C++/JNI tests 覆盖批次编码、RPC、取消、复杂类型、schema 和 reader 生命周期，直接通过上述 CTest 命令执行，不依赖 TPC-H 数据。生产路径没有 JNI fragment 提交或 root/status callback。

```sh
mvn -pl exec/java-exec -am package \
  -Dtest=TestHomogeneousNativeLifecycle,TestNativeArrays,TestNativeDicts,TestNativeOptionalMaps,TestNativeTemporalBinary \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Ddrill.native.engine.library="$PWD/.tools/native-runtime/build/libdrill_native_engine.so"
```

这些 Java JNI tests 需要显式设置 engine library；未设置时会跳过 native 部分。

[通用 plugin SQL 集成测试](../contrib/storage-jdbc/src/test/java/org/apache/drill/exec/store/jdbc/TestNativeGenericPluginCompatibility.java)覆盖未改源码的 CSV/JSON/Parquet、JDBC/H2、sys 与 information_schema。执行时设置 `drill.native.plugin_compat.output`，并用 `DRILL_NATIVE_PLAN_DIR` / `DRILL_NATIVE_STATS_DIR` 指定该输出下 plans/stats 目录。

## 源码提交范围

此变更包含 native 执行、JNI scan、必要回归 tests、构建文件和设计说明。TPC-H SQL、大数据生成、计时比较和 benchmark 集群启动工具留在实验工作区。Dependency cache、数据文件、profiles、运行结果、日志和历史 PoC 不随源码提交。旧 Velox4J runtime、Java fragment wrapper/fallback、完整 worker bootstrap、独立 C++ worker/ZooKeeper C 注册，以及 native scan/SDK/provider 代码已移除。
