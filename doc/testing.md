# 测试与能力验证

本文汇总 sweet 的离线测试、真实 Nacos 验证、能力证据矩阵，以及版本升级与依赖检查。快速运行入口见[顶层 README](../README.md)。

## 真实 Nacos 环境

- Nacos 3.x，**只在 3.2.3 上验过**（钉的是本地 checkout 的 tag 与 commit，写在 `nacos/doc.go` 顶部）。ephemeral 实例的注册、查询、订阅、推送**全程走 gRPC**。
- 连接地址是 SDK gRPC 口：`9848 = mainPort(8848) + 1000`，不是 HTTP 的 8848。Nacos 连接目前走明文 gRPC，暂不提供 TLS 配置面。
- RPC 传输由 grpc-go 负责，不另做 HTTP RPC 传输；Nacos 配置的读侧与写侧都走 gRPC（`Query` / `Watch` / `Publish` / `Remove`），不经 admin HTTP 口。
- 服务端**开鉴权也能连**。凭据走 `Options.Access`（`AccessGetter`），注入在**每个请求**的 `Metadata.headers` 上——建连时服务端对 `ServerCheckRequest` 直接 return、不进 handler 也不进 filter 链（`GrpcRequestAcceptor.java:106-115`），校验点在每次派发上（`RequestHandler.handleRequest:45-56`）。
- 包内自带 `PasswordAccess`（HTTP 登录，默认路径 `/v3/auth/user/login`，token 缓存 + TTL 窗口内刷新 + 被 403 作废后重取一次）和 `StaticAccess`（直接给 header）。两半都在真 3.2.3 上读过：不带 `Access` 时握手仍成功、config 查询落 `ErrDenied`；带 `PasswordAccess` 时路由热更新全程不重启，读数见 [Nacos 协议加固](nacos-sdk-hardening.md) 的判据 10。
- 连真实例的那轮自证测试不配凭据，只读 `NACOS_GRPC_ADDR`，所以运行时服务端要设 `nacos.core.auth.enabled=false`；开鉴权的那轮是在隔离实例上手工验证的。
- 用嵌入式 Derby 时它是单实例锁：再启一个 standalone 会报 `XSDB6`；正确停法是 `bin/shutdown.sh`，进程存活时不要删 `db.lck`。多实例运行的额外注意事项见 [Nacos 协议加固](nacos-sdk-hardening.md)。

## 能力与边界

每条主张都指出是谁在看着它，没有测试钉着的不写。

| 主张 | 钉住它的测试 | 实测读数 |
|---|---|---|
| 注册 / 注销 / 查询 / 订阅 + 推送 + ACK | `TestBackendsContract`（`registry/nacos/harbor_test.go`） | 同一份客户端代码对 Nacos 与 harbor 两个服务端都成立，无 harbor 分支 |
| 建连：先一元 `ServerCheckRequest` 拿 connectionId，再开 bidi | `TestLiveSetupHandshakeIsAcknowledged`（`nacos/integration_test.go`） | 拿回服务端能力表 |
| 保活探针与死流检测 | `TestLiveProbeChannelFacts`（`nacos/detect_test.go`） | 一元通道有应答、bidi 流上无人应答，两条都断言 |
| ephemeral 实例的活性＝那条 bidi 连接 | `TestIdleRegistrationSurvives`（`registry/nacos/persist_probe_test.go`） | 空载 3 分钟 12 次探测，实例始终在 |
| 服务端重启后重连并**重放注册** | `TestReconnectAfterServerRestart`（同上文件） | 手动跑（`SWEET_RESTART_DRILL=1`，否则 skip），2026-11-04 真机跑过：SIGTERM 重启那套 3.2.3，约 10 秒后 9848 回来；客户端 `21:20:32.189` 观察到注册消失，**16 秒后注册自己回来**，期间没人再调 `Expose`。这条测试原先只等"实例还在"、不重启也 5s 通过，现在要求先观察到消失（见 [Nacos 协议加固](nacos-sdk-hardening.md)） |
| 优雅上下线：先注销、再 drain、再停止 accept | `TestRollingRestartDrill`（`registry/nacos/restart_drill_test.go`） | `failures=0` 是不漂的那半；`calls` 随运行次数变（记录时 `calls=180`、收敛 559.5ms / 611.1ms，2026-11-04 复跑 `calls=175`、收敛 561.14ms / 557.76ms） |
| 长命 goroutine 里的 panic 不拖垮进程 | `TestGuardConvertsPanicToStreamLoss`（`nacos/panic_guard_test.go`） | panic→hook 收到原因，`Ready()` 转 false，重握手后 `Epoch()≥2` |
| 错误可分类 | `TestCauseStaysClassifiableThroughWrapping`（`nacos/errors_test.go`） | 三层包装后 Unavailable / Canceled 都取得到 |
| 与改不了的 grpc-go 桩互通 | `TestNativeClientCallsSweetServer`、`TestNativeServerAcceptsGeneratedStreamViaSweet`、`TestSweetClientDiscoversNativeServer`（`interoptest/`） | stock `grpc.health.v1.Health` 的一元与生成式流方法都通，两个方向都测 |
| 内置 gRPC 反射与健康服务 | `TestNativeClientCallsSweetServer`、`TestReflectionListsRegisteredService` | 见 [gRPC 兼容与标准工具](grpc-interop.md) 的 grpcurl 外部实测 |
| 重试走 gRPC 原生 service config，sweet 不自带重试旋钮 | `TestNativeRetryAppliesThroughSweetDial` + 负对照 `TestWithoutRetryConfigTheInjectionSticks` | 配 retryPolicy 时服务端看到 3 次尝试、调用成功；不配时只看到 1 次并直接把 UNAVAILABLE 抛给调用方（grpc-go 对 UNAVAILABLE 不做透明重试） |
| 手写 `ServiceDesc` 与生成码一样会走服务端一元拦截器 | `TestUnaryHandlerInvokesTheInterceptor` | grpc-go 把拦截器链交给 desc 的 handler 去调用；手写 desc 若丢弃第 4 个参数，服务端一元拦截器会全部静默失效 |
| Nacos 不可达时的快照回落，**默认关** | `TestWatchFallsBackToSnapshot` + 负对照 `TestWatchWithoutSnapshotDirStillFails` | 设了 `SnapshotDir` 才在冷拨号或订阅失败时投递带 age 的旧列表并回调通知；不设时同一场景仍然报错。另有真停掉本机 Nacos 的冷启动端到端，见 [Nacos 快照回落](nacos-snapshot.md) |
| 配置的**读侧**：notify 只带 key，客户端回拉内容并用新 md5 重新 listen；重连后 re-listen | `TestConfigWatchPullsAfterNotify`、`TestListenResponseStaleSubsetIsPulled`、`TestConfigReplayAfterReconnect`（`nacos/config_client_test.go`） | 真机（本地 3.2.3、`9848`，一次运行、全程未重启）发布 2 条路由表后 2s 报 `applied 2 route(s)`；同一天补跑验的是删除方向——表 A 换表 C 后旧路径 404、新路径 200，且"配置本来不存在 → 首次发布"也会被 notify→回拉→装上。读数在 [Nacos 网关路由](nacos-config-routes.md) |
| 配置的**写侧**：`ConfigPublishRequest` / `ConfigRemoveRequest` 走同一条 SDK 口，含 CAS（`casMd5`） | `TestConfigPublishWireShape`、`TestConfigPublishLeavesOutCasAndAddition`、`TestConfigRemoveSendsNoTag`、`TestConfigPublishCasConflictIsErrConflict`、`TestConfigPublishRefusalIsNotAConflict`（`nacos/config_write_test.go`）+ 四条真机 `TestLivePublish*`（`nacos/config_write_live_test.go`） | 2026-11-14 真机（3.2.3 standalone、`9848`）：发→读回逐字相同→删→not-found→再删仍幂等；错 md5 的 CAS 报 `errorCode=500` + `Cas publish fail, server md5 may have changed.`（专属错误码不上线，只能靠 message 分类）；对 md5 的 CAS 成功且 md5 移动；CAS 对不存在的 key 仍创建成功。**写完不立即可读**：新 dataId 58–118ms、删除后 85–110ms、覆盖写 83ms，因为读侧只查内存 cache 而 dump 是异步的。读数在 [Nacos 网关路由](nacos-config-routes.md) 的「写侧补全」 |

## 离线测试

```bash
go test ./...
```

一次实测输出：

```text
ok  	github.com/javahongxi/sweet/example/helloworld/ping	0.334s
ok  	github.com/javahongxi/sweet/interoptest	0.436s
ok  	github.com/javahongxi/sweet/nacos	(cached)
ok  	github.com/javahongxi/sweet/registry/nacos	0.411s
ok  	github.com/javahongxi/sweet/sweettest	0.352s
```

包名与结果之间是制表符，最后一列是耗时；命中缓存的那一项显示 `(cached)`。

## 真实 Nacos 测试

连真实 Nacos 的测试用环境变量打开，不设就 skip，不会假装跑过：

```bash
NACOS_GRPC_ADDR=127.0.0.1:9848 go test ./nacos ./registry/nacos ./interoptest -count=1
```

**换了 Nacos 服务端版本**（升级实例、或改成线上那套）时，先跑下面这批直接把线级事实交给真服务端核对的测试。它们不要凭据，不含 3 分钟空载探针，也不含需要手动重启服务的 drill：

```bash
NACOS_GRPC_ADDR=127.0.0.1:9848 go test ./nacos ./registry/nacos -count=1 -run Live
```

选中的是 `TestLivePublishQueryRemoveRoundTrip`、`TestLivePublishCas`、
`TestLivePublishCasCreatesAbsent`、`TestLivePublishBlankContentIsRejected`（config 写侧，会在
真服务端上建一批一次性 dataId 并删干净）、`TestLiveSetupHandshakeIsAcknowledged`（握手能力表）、
`TestLiveUnaryServerCheck`、`TestLiveSetupIsNotTornDownByCallerTimeout`、
`TestLiveProbeChannelFacts`（探针通道事实）、`TestLiveRegisterQueryDeregister`、
`TestLiveWatchSeedsAndPushes`、`TestLiveWatchSharedSnapshotAndCancellation`。这份清单可用下面的命令核对，不需要连接服务端：

```bash
go test -list 'Live' ./nacos ./registry/nacos
```

快速验证核心功能，跳过 3 个长时测试：

```bash
NACOS_GRPC_ADDR=127.0.0.1:9848 go test -short ./nacos ./registry/nacos ./interoptest -count=1
```

`-short` 模式跳过：

- `TestIdleRegistrationSurvives`：≥3 分钟空载探针。
- `TestReconnectAfterServerRestart`：≤6 分钟，需手动重启 Nacos。
- `TestRollingRestartDrill`：约 2 分钟，需预编译服务端二进制。

不设 `-run` 也不加 `-short` 的那条是全量：会多花 3 分钟空载，并带上 `interoptest` 里经 Nacos 发现第三方 grpc-go 服务端的 `TestSweetClientDiscoversNativeServer`。它不叫 `Live`，所以 `-run Live` 那轮不含它。

对端是 harbor 的四条测试要另给 `SWEET_HARBOR_ADDR`，包括 `nacos/harbor_test.go` 两条与 `registry/nacos/harbor_test.go` 两条。

## 版本与依赖验证

当前最新稳定版本为 `v1.0.1`，`go get github.com/javahongxi/sweet@v1.0.1` 即可；`@latest` 也会解析到该 tag。升级 `grpc-go` 或者换 Nacos 服务端版本之后，把上面的离线、Live 与快速验证命令都跑一遍，别只跑离线测试。

不拆 module 不等于别人要为整个 sweet 买单：`go get` 下的是整仓，编译进去的只有 import 的那条链。只用 nacos 时参与编译的是 `nacos` 与 `gen` 两个包，`registry`/`gateway`/`server` 一行都不进二进制，第三方依赖也只有 `grpc` 自己那一组（`x/net`、`x/sys`、`x/text`、`genproto`、`protobuf` 都是它的依赖）。在自己的模块里可以这样验证：

```bash
go list -deps github.com/javahongxi/sweet/nacos | grep javahongxi/sweet
go build -o /tmp/app . && go tool nm /tmp/app | grep -o 'javahongxi/sweet/[a-z0-9_]*' | sort | uniq -c
```

第一条应只列出 `gen` 与 `nacos` 两行。

真正的代价在另一处：`go.mod` 的 require 列表是整仓共享的。今天全仓只依赖 `grpc`，所以没有额外成本；哪天 `gateway` 或 `server` 引入新的第三方库，它会进每一个"只用 nacos"的人的 module graph，即使仍然不进他的二进制。
