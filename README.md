# sweet

基于 gRPC 的 Go RPC 框架，用 Nacos 做注册中心，手写服务注册与发现，不依赖 nacos-sdk-go。

传输与线格式一律用 `grpc-go`：sweet 不重新发明 RPC 线格式，需要跨语言互通时直接用 gRPC。

## 是什么 / 不是什么

- **是**：Go 侧的服务发现 + 调用面。你写的仍然是标准 gRPC 桩，sweet 负责"按服务名找到对端"
  和"上下线真的生效"这两件事。
- **面向**：要接 Nacos 3.x 的 Go 服务；以及想知道"手写命名协议客户端到底要处理什么"的人
  （协议层与适配层的取舍写在 `registry/nacos/nacos.go` 与 `nacos/conn.go` 的注释里）。
- **不做**：HTTP 传输、配置的写路径（发布/删除走 Nacos 自己的 admin HTTP 口，不在本库）、TLS 配置面
  （连 Nacos 那条 gRPC 走明文，`nacos/conn.go` 里写死 `insecure`）。

## 要求与兼容

- Go ≥ 1.27（`go.mod` 声明 `go 1.27.3`）
- 唯一第三方依赖：`google.golang.org/grpc v1.84.0`（`google.golang.org/protobuf` 是它自身的依赖，不算第二个库）
- 内置的反射与健康服务同样来自 `grpc-go`（`google.golang.org/grpc/reflection`、`.../health`），依赖清单不因它们变长
- Nacos 3.x，**只在 3.2.3 上验过**（钉的是本地 checkout 的 tag 与 commit，写在 `nacos/doc.go` 顶部）。
  ephemeral 实例的注册、查询、订阅、推送**全程走 gRPC**
- 连接地址是 SDK gRPC 口：`9848 = mainPort(8848) + 1000`，不是 HTTP 的 8848
- 服务端**开鉴权也能连**。凭据走 `Options.Access`（`AccessGetter`），注入在**每个请求**的
  `Metadata.headers` 上——建连时服务端对 `ServerCheckRequest` 直接 return、不进 handler 也不进 filter 链
  （`GrpcRequestAcceptor.java:106-115`），校验点在每次派发上（`RequestHandler.handleRequest:45-56`）。
  包内自带 `PasswordAccess`（HTTP 登录，默认路径 `/v3/auth/user/login`，token 缓存 + TTL 窗口内刷新 +
  被 403 作废后重取一次）和 `StaticAccess`（直接给 header）。两半都在真 3.2.3 上读过：不带 `Access`
  时握手仍成功、config 查询落 `ErrDenied`；带 `PasswordAccess` 时路由热更新全程不重启
  （读数在 `doc/nacos-sdk-hardening.md` 的判据 10 那节）
- 但**连真实例的那轮自证测试不配凭据**（只读 `NACOS_GRPC_ADDR`），所以跑它的时候服务端要
  `nacos.core.auth.enabled=false`；开鉴权的那轮是手工在隔离实例上做的，见上一条
- 用嵌入式 Derby 时它是单实例锁：再启一个 standalone 会报 `XSDB6`；正确停法是 `bin/shutdown.sh`，进程存活时不要删 `db.lck`

## 五分钟跑通

在自己的模块里引入。模块路径就是仓库地址，不用 clone、不用 `replace`：

```
go get github.com/javahongxi/sweet   # 尚无 tag，装的是 pseudo-version；钉住写 @main 或 @<commit>
go mod tidy                          # 补齐 go.sum，跳过这步 build 会报 missing go.sum entry
```

只想试一把，就在本仓库里直接编两个命令出来（不需要 Nacos 时走 direct：纯直通、不做发现，
客户端自己给地址）：

```
go build -o /tmp/sweet-server ./example/helloworld/cmd/server
go build -o /tmp/sweet-client ./example/helloworld/cmd/client

/tmp/sweet-server -registry=direct -listen 127.0.0.1:20000 &
/tmp/sweet-client -registry=direct -endpoints 127.0.0.1:20000
```

实测输出：

```
registry=direct is a passthrough: clients should dial -endpoints 127.0.0.1:20000
sweet server up: listen=127.0.0.1:20000 advertised=127.0.0.1:20000 service=sweet.example.v1.Ping registry=direct
resolved sweet.example.v1.Ping through direct registry
Ping       <= "pong"
Events     <= 1 "a"
Events     <= 2 "b"
Events     <= 3 "c"
Events     == 3 frames, closed by server
```

接上 Nacos 时走 nacos（客户端不再需要地址，只认服务名）：

```
/tmp/sweet-server -registry=nacos -server 127.0.0.1:9848 -listen 127.0.0.1:20000 &
/tmp/sweet-client -registry=nacos -server 127.0.0.1:9848
```

实测输出：

```
sweet server up: listen=127.0.0.1:20000 advertised=127.0.0.1:20000 service=sweet.example.v1.Ping registry=nacos
resolved sweet.example.v1.Ping through nacos registry
Ping       <= "pong"
Events     <= 1 "a"
Events     <= 2 "b"
Events     <= 3 "c"
Events     == 3 frames, closed by server
```

上面两块都只贴到一元与服务端流为止（ contiguous 前缀，没删中间行）；这个例子也有双向流
`Echo` 与客户端流 `Ack`，四形态的完整输出见 `example/helloworld/README.md`。

`Ctrl-C`（或 `kill`）掉服务端后再拨一次，能看到优雅下线的结果——实例先注销、客户端随即拿不到地址：

```
Ping: rpc error: code = Unavailable desc = name resolver error: produced zero addresses
```

### 跨语言：调 jaws 的 Java gRPC 服务端

对端是 jaws 的 Spring Boot wire provider（`WireBootProvider`），调用侧是 `greeter.proto`
生成的标准 Go 桩，只把 `conn` 换成 `client.Dial` 给的。默认经 Nacos 拿地址（jaws 把它注册成
`jaws/<接口全名>`，所以查找名与 gRPC 服务名是两串字符串），`-registry=direct` 则只要线格式
互通。细节与两种模式的实测见 `example/jaws-wire/README.md`，直连那发：

```
go run ./example/jaws-wire -registry=direct -name sweet
calling /greeter.Greeter/SayHello against direct found as "greeter.Greeter"
unary  SayHello        → "Hello, sweet! (from jaws-wire-boot)"
stream SayHelloStream → "Hello #1, sweet! (from jaws-wire-boot stream)"
stream SayHelloStream → "Hello #2, sweet! (from jaws-wire-boot stream)"
stream SayHelloStream → "Hello #3, sweet! (from jaws-wire-boot stream)"
```

## 用法

### 服务端

```go
srv, err := server.New(server.Options{
    Address:     "127.0.0.1:0",     // ":0" 由系统分配端口，注册时上报实际端口
    ServiceName: "greeter.Greeter", // 留空则取 Register 时 ServiceDesc 的名字；此时再注册第二个不同名的服务会被拒
    Weight:      100,               // 仅随实例透传给对端，本客户端不据此分流量
    Metadata:    map[string]string{"version": "1.0.0"},
    Registry:    reg,
    DrainDelay:  200 * time.Millisecond, // Stop 在注销后、停止 accept 前额外等待
})

srv.Register(&pb.Greeter_ServiceDesc, impl)  // 用生成码的 ServiceDesc，不另造服务标识
go func() { _ = srv.Serve() }()              // Serve 阻塞，直到 Stop 才返回
if err := srv.Listen(); err != nil { ... }   // 同步点：返回 nil 后 Addr()/Port() 才可读
defer srv.Stop()                             // 注销 → 等 drain → GracefulStop
```

两个容易踩的点：`Serve()` 是阻塞的；注册发生在开始 accept **之前**，所以那一小段窗口里对端能解析到一个**连得上、但没人读**的地址——调用挂到自己的 deadline 报 `DEADLINE_EXCEEDED`（不是 connection refused），`grpc.Serve` 起来后同一条 `ClientConn` 立刻成功。次序保持现状是定案，实测与 go-zero / dubbo-go / jaws 的对照见 `doc/service-publish-order.md`。

### 客户端

```go
conn, err := client.Dial(reg, "greeter.Greeter",
    grpc.WithTransportCredentials(insecure.NewCredentials()))
cli := pb.NewGreeterClient(conn)   // 生成码客户端直接用，sweet 不改写 :path
```

目标串由 `Dial` 拼成 `reg.Name() + ":///" + serviceName`，不需要手写 URI；resolver builder 按连接注入（`grpc.WithResolvers`），不注册进程全局，因此一个进程里可以并存多套注册中心。

### 注册中心

```go
type Registry interface {
    Name() string
    Expose(ctx context.Context, serviceName string, in Instance) (func(), error)
    Watch(ctx context.Context, serviceName string) (<-chan []Instance, error)
}
```

两个实现：`registry/nacos`（把下面那套手写协议包成 `Registry`）与 `registry/direct`（直通，兼测试替身）。协议本身在 `nacos` 包里（信封、握手、保活探针、重连钩子、推送与 ACK），`registry/nacos` 只做适配与重放。`Expose` 返回注销闭包；`Watch` 首次调用即订阅，返回的第一个快照来自订阅响应本身（Nacos 的 subscribe 会带回当前实例列表，不需要额外查一次）。

### 超时、取消与错误

deadline 与取消走 `context`，sweet 不发明自己的超时字段；其余 grpc 惯用法（dial option、
call option、keepalive）通过 `Dial` 与 `Options.ServerOptions` 原样透传。框架自己包过的错误
（连接、探针、订阅）保持可分类——`%w` 一路包上去之后调用方仍能用 `errors.As` 取到 gRPC 状态码，
这条由 `TestCauseStaysClassifiableThroughWrapping` 钉住。

## 能力与边界

每条主张都指出是谁在看着它，没有测试钉着的不写。

| 主张 | 钉住它的测试 | 实测读数 |
|---|---|---|
| 注册 / 注销 / 查询 / 订阅 + 推送 + ACK | `TestBackendsContract`（`registry/nacos/harbor_test.go`） | 同一份客户端代码对 Nacos 与 harbor 两个服务端都成立，无 harbor 分支 |
| 建连：先一元 `ServerCheckRequest` 拿 connectionId，再开 bidi | `TestLiveSetupHandshakeIsAcknowledged`（`nacos/integration_test.go`） | 拿回服务端能力表 |
| 保活探针与死流检测 | `TestLiveProbeChannelFacts`（`nacos/detect_test.go`） | 一元通道有应答、bidi 流上无人应答，两条都断言 |
| ephemeral 实例的活性＝那条 bidi 连接 | `TestIdleRegistrationSurvives`（`registry/nacos/persist_probe_test.go`） | 空载 3 分钟 12 次探测，实例始终在 |
| 服务端重启后重连并**重放注册** | `TestReconnectAfterServerRestart`（同上文件） | 手动跑（`SWEET_RESTART_DRILL=1`，否则 skip），2026-11-04 真机跑过：SIGTERM 重启那套 3.2.3，约 10 秒后 9848 回来；客户端 `21:20:32.189` 观察到注册消失，**16 秒后注册自己回来**，期间没人再调 `Expose`。这条测试原先只等"实例还在"、不重启也 5s 通过，现在要求先观察到消失（见 `doc/nacos-sdk-hardening.md`）|
| 优雅上下线：先注销、再 drain、再停止 accept | `TestRollingRestartDrill`（`registry/nacos/restart_drill_test.go`） | `failures=0` 是不漂的那半；`calls` 随运行次数变（记录时 `calls=180`、收敛 559.5ms / 611.1ms，2026-11-04 复跑 `calls=175`、收敛 561.14ms / 557.76ms）|
| 长命 goroutine 里的 panic 不拖垮进程 | `TestGuardConvertsPanicToStreamLoss`（`nacos/panic_guard_test.go`） | panic→hook 收到原因，`Ready()` 转 false，重握手后 `Epoch()≥2` |
| 错误可分类 | `TestCauseStaysClassifiableThroughWrapping`（`nacos/errors_test.go`） | 三层包装后 Unavailable / Canceled 都取得到 |
| 与改不了的 grpc-go 桩互通 | `TestNativeClientCallsSweetServer`、`TestNativeServerAcceptsGeneratedStreamViaSweet`、`TestSweetClientDiscoversNativeServer`（`interoptest/`） | stock `grpc.health.v1.Health` 的一元与生成式流方法都通，两个方向都测 |
| 内置 gRPC 反射与健康服务 | `TestNativeClientCallsSweetServer`、`TestReflectionListsRegisteredService` | 见下一节的 grpcurl 外部实测 |
| 重试走 gRPC 原生 service config，sweet 不自带重试旋钮 | `TestNativeRetryAppliesThroughSweetDial` + 负对照 `TestWithoutRetryConfigTheInjectionSticks` | 配 retryPolicy 时服务端看到 3 次尝试、调用成功；不配时只看到 1 次并直接把 UNAVAILABLE 抛给调用方（grpc-go 对 UNAVAILABLE 不做透明重试） |
| 手写 `ServiceDesc` 与生成码一样会走服务端一元拦截器 | `TestUnaryHandlerInvokesTheInterceptor` | grpc-go 把拦截器链交给 desc 的 handler 去调用；手写 desc 若丢弃第 4 个参数，服务端一元拦截器会全部静默失效 |
| Nacos 不可达时的快照回落，**默认关** | `TestWatchFallsBackToSnapshot` + 负对照 `TestWatchWithoutSnapshotDirStillFails` | 设了 `SnapshotDir` 才在冷拨号或订阅失败时投递带 age 的旧列表并回调通知；不设时同一场景仍然报错。另有真停掉本机 Nacos 的冷启动端到端（见「注册中心不可达时」）|
| 配置的**读侧**：notify 只带 key，客户端回拉内容并用新 md5 重新 listen；重连后 re-listen | `TestConfigWatchPullsAfterNotify`、`TestListenResponseStaleSubsetIsPulled`、`TestConfigReplayAfterReconnect`（`nacos/config_test.go`） | 真机（本地 3.2.3、`9848`，一次运行、全程未重启）发布 2 条路由表后 2s 报 `applied 2 route(s)`；同一天补跑验的是删除方向——表 A 换表 C 后旧路径 404、新路径 200，且"配置本来不存在 → 首次发布"也会被 notify→回拉→装上。读数在 `doc/nacos-config-routes.md`。**写路径不经本库**：Nacos 3.x 已移除 v1 HTTP 口，发布/删除走 `/nacos/v3/admin/cs/config`（参数名 `groupName`）|

## 标准工具能直接看见它

`server.New` 顺手注册了 gRPC 反射与健康服务——两者都在 `grpc-go` 自己仓库里，不构成第二个
依赖。意义不在"多了两个服务"，而在**对端不需要知道 sweet 存在**：`grpcurl` 这类标准工具能自己
列出实例上有什么服务、读出 schema、直接调一刀。实测（2026-10-28，`grpcurl 1.9.4`，对端
`go run ./example/greeter/cmd/server -registry=direct -listen 127.0.0.1:20001`）：

```
$ grpcurl -plaintext localhost:20001 list
grpc.health.v1.Health
grpc.reflection.v1.ServerReflection
grpc.reflection.v1alpha.ServerReflection
sweet.example.v1.Greeter

$ grpcurl -plaintext -d '{"name":"grpcurl","times":2}' localhost:20001 sweet.example.v1.Greeter/GreetStream
{
  "message": "Hello #1, grpcurl!"
}
{
  "message": "Hello #2, grpcurl!"
}
```

`describe sweet.example.v1.Greeter` 会把四个 rpc 的完整 schema 打出来（含 `stream` 标注），
说明列出来的不只是名字。

健康状态跟着 sweet 的生命周期走，而不是恒为 `SERVING`：`grpc-go` 的 `health.NewServer()` 一
开始就把总体服务标成 SERVING，而那时还没监听，所以 `New` 里显式改成 `NOT_SERVING`，直到
`Serve` 绑好端口并 `Expose` 成功才翻成 SERVING（注册的每个服务名各自一份状态）；`Stop` 一开始
就 `Shutdown()`，比注销更早一步告诉探活方"别再发流量了"。

## 注册中心不可达时

进程**运行中**断连：重连后重放注册与订阅意图，这条路有测试钉（见上表）。
进程**新启动**时 Nacos 不可达：默认仍然拿不到实例列表、直接失败——`Options.SnapshotDir` 不设时，
sweet 一个字节都不读也不写。

设置 `SnapshotDir` 才开启盘上回落，规则刻意比两套官方客户端都紧：

- 只在**冷拨号或订阅失败时**才用快照，正常路径永远以服务端为准；空列表一律不落盘——一次抖动的
  空答案不该固化成"谁都不许连"。
- 服务名放进文件名装不下（`jaws/xxx` 这种带 `/` 的名字），所以文件里带 `service/group/namespace`
  三元组，读时对不上就拒绝——拿别人的实例列表比拿不到更糟；没有时间戳的列表也拒绝，年龄是这条路
  的全部意义。
- 陈旧必须显式可见：日志一行 WARN，同时回调 `OnSnapshotFallback(service, instances, age)`，另有
  `SnapshotAge(service)` 给调用方和测试直接问。
- 回落后订阅意图保持 live 并启动重连循环，Nacos 恢复时重放会重新订阅、用新列表替换旧列表。它撑过
  的是空窗期，不是"绕过注册中心"。

对照过 nacos 的两套官方客户端，它们并不一致，所以别把任何一套当成"Nacos 本来就这样"：

- **Java `nacos-client`（3.2.3）**：`ServiceInfoHolder` 构造时可以从盘上预载实例表
  （`client/.../naming/cache/ServiceInfoHolder.java:67-71`；`getServiceInfo` 只读这份内存 map，
  `:105-109`），所以 server 全挂时重启仍能拿到上次落盘的列表。但预载开关
  `NAMING_LOAD_CACHE_AT_START` **默认 false**（`:81-89`），且救的是查询类调用——
  `doSubscribe` 只在重复订阅时用当前缓存通知（`NacosNamingService.java:739-746`），随后照旧发
  RPC、失败照旧抛（`:546`）。运维侧另有放文件即生效的 `FailoverReactor`
  （`backups/FailoverReactor.java` + `DiskFailoverDataSource`）。
- **Go `nacos-sdk-go` v2.3.5**：预载默认是开的（`naming_cache/service_info_holder.go:54,60-61`），
  但那份缓存没有 TTL、也没人校验 `Checksum`；运行时 `Subscribe`/查询失败一律 `return err`
  不回落（`naming_grpc/naming_grpc_proxy.go:176-189`）；注册意图的 redo 队列是纯内存
  `ConcurrentMap`（`connection_event_listener.go:33-34`），进程重启即清零；命名侧的 failover
  至今是那句 `//todo FailoverReactor`（`service_info_holder.go:110`）。

已测到哪一层：`registry/nacos/snapshot_test.go` 六条用例钉住规则，含负对照——同一个不可达的服务端，
不配 `SnapshotDir` 时必须仍然失败。另有一条**真的把本机 Nacos 停掉**的冷启动端到端（provider 全程
在线，所以"回落后能不能调通"是实测而不是推断）。不带 `-snapshot` 的冷启动直接失败，错误链原样交给
调用方：

```
Greet: nacos: subscribe sweet.example.v1.Greeter: nacos: handshake with 127.0.0.1:9848: nacos: server check 127.0.0.1:9848: rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: dial tcp 127.0.0.1:9848: connect: connection refused"
```

带 `-snapshot` 的同一场景，整份输出如下（先声明陈旧，再四种形态全部调通；Nacos 在 hold 期间被拉起，
hold 结束那一发仍走通了）：

```
serving sweet.example.v1.Greeter from snapshot: 1 instance(s), 0s old — addresses may be stale
resolved sweet.example.v1.Greeter through nacos registry
Greet       <= "Hello, sweet!"
GreetStream <= 1 "Hello #1, sweet!"
GreetStream <= 2 "Hello #2, sweet!"
GreetStream <= 3 "Hello #3, sweet!"
GreetStream == 3 frames, closed by server
GreetChat   <= 1 "Hello, a!"
GreetChat   <= 2 "Hello, b!"
GreetChat   <= 3 "Hello, c!"
GreetChat   == 3 frames from 3 requests, closed by server
GreetAll    <= "Hello, a, b, c!"
GreetAll    == 1 frame from 3 requests
holding 80s: restart Nacos now to watch the snapshot fallback heal
after hold  <= "Hello, sweet!"
```

恢复也不靠重启进程：那份快照的 `savedAt` 从 `2026-10-28T06:03:43.369088+08:00` 被重订阅后的新列表
刷成 `2026-10-28T06:04:05.001761+08:00`。`0s old` 是因为这条端到端里快照是同一次脚本几十秒前刚播种
的，真出故障时这里是分钟级。三行合一句：回落撑住的是空窗期，服务端一回来就自己回到以服务端为准。

## 深入了解更多

- `example/helloworld/README.md`——手写 `grpc.ServiceDesc` 跑通四种形态，不碰 protoc。
- `example/greeter/README.md`——proto 生成码的 Go 调 Go，四种形态；末尾有它和 helloworld 的
  逐项对照表，两份代码的差别只有"手写 vs 生成"。
- `example/jaws-wire/README.md`——Go 客户端调 Java 服务端（jaws 的 wire provider），跨语言实测。
- 反方向的样例在 jaws 仓库：`jaws-samples/jaws-sample-wire-sweet`，jaws 的 wire 客户端经 Nacos
  发现并调用 sweet 起的 grpc-go 服务。
- 取舍写在代码注释里，两处最值钱：`registry/nacos/nacos.go` 的文件头（ephemeral 这一条路径的
  范围、"活性＝那条 bidi 连接、没有 beat"、§8.6 的"注销优先"与重放次序：实例先、订阅后）；
  `nacos/conn.go`（对 nacos-client 契约的超时取值、推送必须回同一个 `requestId` 否则服务端重推、
  "丢流不等于丢通道"的重连语义）。`nacos/envelope.go` 与 `nacos/naming.go` 记的是信封和
  `Metadata.type` 到 Java 类名的映射。
- vendored proto 与重生成的规矩在 `gen/doc.go`（重生成后 `cmp` 逐字节一致才算数）。

## 测试

```
go test ./...                                    # 离线，用进程内协议桩
```

```
ok  	github.com/javahongxi/sweet/example/helloworld/ping	0.334s
ok  	github.com/javahongxi/sweet/interoptest	0.436s
ok  	github.com/javahongxi/sweet/nacos	(cached)
ok  	github.com/javahongxi/sweet/registry/nacos	0.411s
ok  	github.com/javahongxi/sweet/sweettest	0.352s
```

（原文照抄：包名与结果之间是制表符，最后一列是耗时；命中缓存的那一项显示 `(cached)`。）

连真实 Nacos 的测试用环境变量打开（不设就 skip，不会假装跑过）：

```
NACOS_GRPC_ADDR=127.0.0.1:9848 go test ./nacos ./registry/nacos ./interoptest -count=1
```

**换了 Nacos 服务端版本**（升级实例、或改成你线上那套）时，先跑这六条——它们是直接把线级事实
交给真服务端核对的那批，不要凭据、不含 3 分钟空载探针，也不含要人手动重启服务的 drill：

```
NACOS_GRPC_ADDR=127.0.0.1:9848 go test ./nacos ./registry/nacos -count=1 -run Live
```

选中的是 `TestLiveSetupHandshakeIsAcknowledged`（握手能力表）、`TestLiveUnaryServerCheck`、
`TestLiveSetupIsNotTornDownByCallerTimeout`、`TestLiveProbeChannelFacts`（探针通道事实）、
`TestLiveRegisterQueryDeregister`、`TestLiveWatchSeedsAndPushes`。这份清单用
`go test -list 'Live' ./nacos ./registry/nacos` 核过，不连服务端也能核对。

不设 `-run` 的那条是全量：会多花 3 分钟空载（`TestIdleRegistrationSurvives`），并带上
`interoptest` 里那条经 Nacos 发现第三方 grpc-go 服务端的 `TestSweetClientDiscoversNativeServer`
（它不叫 `Live`，所以 `-run Live` 那轮不含它）。对端是 harbor 的那四条要另给 `SWEET_HARBOR_ADDR`
（`nacos/harbor_test.go` 两条 + `registry/nacos/harbor_test.go` 两条）。

## 版本

没有语义化 tag，`go get` 拿到的是 pseudo-version，例如
`v0.0.0-20261027193012-b3fc07f6887d`；`@latest` 也解析到默认分支的这个 pseudo-version（实测）。
要长期钉住就写 `@<commit>`。升级 `grpc-go` 或者换 Nacos 服务端版本之后，把上面几条命令都跑一遍——
离那张表里的主张近，别只跑离线的那条。

不拆 module 不等于别人要为整个 sweet 买单：`go get` 下的是整仓，编译进去的只有你 import 的那条链。
只用 nacos 时参与编译的是 `nacos` 与 `gen` 两个包，`registry`/`gateway`/`server` 一行都不进二进制，
第三方依赖也只有 `grpc` 自己那一组（`x/net`、`x/sys`、`x/text`、`genproto`、`protobuf` 都是它的依赖）。
这三句不用信声明，在自己的模块里跑：

```
go list -deps github.com/javahongxi/sweet/nacos | grep javahongxi/sweet   # 只有 gen 与 nacos 两行
go build -o /tmp/app . && go tool nm /tmp/app | grep -o 'javahongxi/sweet/[a-z0-9_]*' | sort | uniq -c
```

真正的代价在另一处：`go.mod` 的 require 列表是整仓共享的。今天全仓只依赖 `grpc`，所以是零；哪天
`gateway` 或 `server` 引入新的第三方库，它会进每一个"只用 nacos"的人的 module graph，即使仍然不进
他的二进制。

&copy; [hongxi.org](http://hongxi.org)
