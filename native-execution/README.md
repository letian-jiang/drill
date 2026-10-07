# Drill Native Execution（Experimental）

同构 Java Drillbit 内嵌 C++ NativeEngine：Java 保留 SQL 规划、协调和完整 root；native 查询的所有非 root minor 转换为 Velox Task。Scan 使用可选 native provider 或通用 JNI 原 plugin reader，计算不回退到 Java fragment。

[设计方案](DESIGN.md)包含类型映射、minor/driver 调度、两类 JNI 和通信契约；[目录说明](CODE_STRUCTURE.md)定位实现。动态 schema 暂不支持，绑定后变化明确失败。

## 构建与依赖

当前工具链面向 Linux amd64 / AVX2，C++20、JNI JDK、CMake 3.28+、Ninja，以及 Boost/Folly 开发依赖和 RE2。Iceberg SDK 单独使用 C++23/Arrow 工具链。

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
ALTER SESSION SET `exec.native_scan.enabled` = true;
```

`exec.native_fragment.strict` 同样启用全非 root native 模式；没有计算 fallback。关闭 native scan 选项时仍执行 native fragment，只改用原 Java plugin reader。`exec.foreman.root_only` 控制是否排除 Foreman 的非 root placement；单节点须为 false。

## Native scan 开发者接口

Java SubScan 实现 [NativeScanProvider](../exec/java-exec/src/main/java/org/apache/drill/exec/nativeexecution/scan/NativeScanProvider.java)，只提供已分配工作描述：

```java
@Override
public NativeScanProvider.Descriptor nativeScan() {
  return new NativeScanProvider.Descriptor(
      "acme-parquet", 1, readFields, outputFields, pushedFilter, descriptorJson);
}
```

`provider/version` 标识 C++ reader 和 descriptor 格式。`json` 保存当前 minor 的 assigned splits 或其他读取参数；serializer 写入权威的 provider/version/readFields/outputFields/filter。返回 null 或不实现接口，自动使用 generic JNI reader，不要求原 plugin 改写 reader。

C++ provider 实现 [NativeScanPlugin](src/scan/NativeScanPlugin.h)：

```cpp
class AcmeScan final : public drill::nativeexec::NativeScanPlugin {
 public:
  std::string_view provider() const override { return "acme-parquet"; }
  uint32_t descriptorVersion() const override { return 1; }
  NativeScanBinding prepare(const NativeScanRequest& request,
                            const NativeScanContext& context) const override;
};
extern "C" void drill_register_native_scan_plugins_v1(
    drill::nativeexec::NativeScanRegistry* registry) {
  registry->registerPlugin(std::make_shared<AcmeScan>());
}
```

`prepare` 返回固定 schema、每 driver 的 source factory 和可选 normalizeOutput。Filter/output projection 由公共 converter 执行；provider 封装自身表示转换。Factory 拥有 descriptor/work，共享 work 队列使每 split 消费一次。Context 只在 prepare 期间借用；异步 reader 保持 pool 与 buffers 有效。

`BatchSource::next(future)`：有值且 vector 非空为 DATA；有值但 vector 为空为 WAIT，须有有效 future；nullopt 为 EOS。Cancel 可重复调用，失败终止 Task。

外部模块通过 `DRILL_NATIVE_SCAN_PLUGINS` 加载；未知 provider、版本错误和重复注册失败。该接口依赖匹配的 C++/Velox/Folly ABI，extern C 仅保证入口名。[RangeScan 示例](examples/scan/RangeScanPlugin.cpp)和[模块测试](tests/execution/NativeScanPluginTest.cpp)展示真实模块加载与多 driver 工作消费；内建 [Iceberg](src/scan/iceberg/IcebergScan.cpp)使用同一接口。

## 回归验证

C++/JNI tests 覆盖批次编码、RPC、取消、复杂类型、schema、native scan 模块和 reader 生命周期，直接通过上述 CTest 命令执行，不依赖 TPC-H 数据。生产路径没有 JNI fragment 提交或 root/status callback。

```sh
mvn -pl exec/java-exec -am package \
  -Dtest=TestHomogeneousNativeLifecycle,TestNativeArrays,TestNativeDicts,TestNativeOptionalMaps,TestNativeTemporalBinary \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Ddrill.native.engine.library="$PWD/.tools/native-runtime/build/libdrill_native_engine.so"
```

这些 Java JNI tests 需要显式设置 engine library；未设置时会跳过 native 部分。

[通用 plugin SQL 集成测试](../contrib/storage-jdbc/src/test/java/org/apache/drill/exec/store/jdbc/TestNativeGenericPluginCompatibility.java)覆盖未改源码的 CSV/JSON/Parquet、JDBC/H2、sys 与 information_schema。执行时设置 `drill.native.plugin_compat.output`，并用 `DRILL_NATIVE_PLAN_DIR` / `DRILL_NATIVE_STATS_DIR` 指定该输出下 plans/stats 目录。

## 可选 Iceberg SDK

JNI scan 不需要 Iceberg C++ SDK。内建 native Iceberg provider 的独立 reader 使用以下构建入口：

```sh
python3 -m venv .tools/iceberg-python
.tools/iceberg-python/bin/pip install pyarrow==25.0.1
DRILL_NATIVE_PYTHON="$PWD/.tools/iceberg-python/bin/python" \
  native-execution/iceberg/build.sh
```

执行时通过 `DRILL_NATIVE_ICEBERG_LIBRARY` 指定 reader library，并将 SDK 工具链的 `env/lib` 加入 `LD_LIBRARY_PATH`，使用匹配的 C++ runtime。保留的 [SDK C ABI 测试](tests/scan/test_iceberg_task_reader.py)自动生成临时小数据，验证 split、partition constants、delete、Arrow ownership 与错误处理：

```sh
export LD_LIBRARY_PATH="$PWD/.tools/iceberg-toolchain/env/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export DRILL_NATIVE_ICEBERG_LIBRARY="$PWD/.tools/iceberg-build/libdrill_iceberg_reader.so"
.tools/iceberg-python/bin/python native-execution/tests/scan/test_iceberg_task_reader.py \
  --library "$DRILL_NATIVE_ICEBERG_LIBRARY"
```

## 源码提交范围

此变更包含执行代码、接口、必要回归 tests、构建文件和设计说明。TPC-H SQL、大数据生成、计时比较和 benchmark 集群启动工具留在实验工作区。Dependency cache、数据文件、profiles、运行结果、日志和历史 PoC 不随源码提交。旧 Velox4J runtime、Java fragment wrapper/fallback、完整 worker bootstrap 和独立 C++ worker/ZooKeeper C 注册代码已移除。
