# sweet

基于 gRPC 的 Go RPC 框架，用 Nacos 做注册中心，手写服务注册与发现，不依赖 nacos-sdk-go。

传输与线格式一律用 `grpc-go`：sweet 不重新发明 RPC 线格式，需要跨语言互通时直接用 gRPC。

## 是什么 / 不是什么

- **是**：Go 侧的服务发现 + 调用面。你写的仍然是标准 gRPC 桩，sweet 负责"按服务名找到对端"
  和"上下线真的生效"这两件事。
- **面向**：要接 Nacos 3.x 的 Go 服务；以及想知道"手写命名协议客户端到底要处理什么"的人
  （协议层与适配层的取舍写在 `registry/nacos/nacos.go` 与 `nacos/conn.go` 的注释里）。
- **不做**：HTTP/REST 传输、配置中心、鉴权与 TLS 配置面、持久实例（只支持 ephemeral）、
  加权负载均衡（`Weight` 只随实例透传给对端，本客户端不据此分流量）。

## 要求与兼容

- Go ≥ 1.27（`go.mod` 声明 `go 1.27.3`）
- 唯一第三方依赖：`google.golang.org/grpc v1.84.0`（`google.golang.org/protobuf` 是它自身的依赖，不算第二个库）
- 内置的反射与健康服务同样来自 `grpc-go`（`google.golang.org/grpc/reflection`、`.../health`），依赖清单不因它们变长
- Nacos 3.x。ephemeral 实例的注册、查询、订阅、推送**全程走 gRPC**
- 连接地址是 SDK gRPC 口：`9848 = mainPort(8848) + 1000`，不是 HTTP 的 8848
- 服务端鉴权需关闭（`nacos.core.auth.enabled=false`）
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

对端是 jaws 的 `WireProvider`（一行都改不了），调用侧是 `greeter.proto` 生成的标准 Go 桩，
只把 `conn` 换成 `client.Dial` 给的。跑法与实测输出见 `example/jaws-wire/README.md`：

```
go run ./example/jaws-wire -name sweet
unary  SayHello        → "Hello, sweet! (from jaws-wire)"
stream SayHelloStream → "Hello #1, sweet! (from jaws-wire stream)"
```

## 用法

### 服务端

```go
srv, err := server.New(server.Options{
    Address:     "127.0.0.1:0",     // ":0" 由系统分配端口，注册时上报实际端口
    ServiceName: "greeter.Greeter", // 留空则取 Register 时 ServiceDesc 的名字
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

两个容易踩的点：`Serve()` 是阻塞的；注册发生在开始 accept **之前**，所以对端不会解析到一个还没就绪的地址。

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
| 服务端重启后重连并重放注册与订阅 | `TestReconnectAfterServerRestart`（同上文件） | 手动跑：要 `SWEET_RESTART_DRILL=1` 并由人重启 Nacos，否则 skip——它不在自动跑的那轮里 |
| 优雅上下线：先注销、再 drain、再停止 accept | `TestRollingRestartDrill`（`registry/nacos/restart_drill_test.go`） | `calls=180 failures=0`，收敛 559.5ms / 611.1ms |
| 长命 goroutine 里的 panic 不拖垮进程 | `TestGuardConvertsPanicToStreamLoss`（`nacos/panic_guard_test.go`） | panic→hook 收到原因，`Ready()` 转 false，重握手后 `Epoch()≥2` |
| 错误可分类 | `TestCauseStaysClassifiableThroughWrapping`（`nacos/errors_test.go`） | 三层包装后 Unavailable / Canceled 都取得到 |
| 与改不了的 grpc-go 桩互通 | `TestNativeClientCallsSweetServer`、`TestNativeServerAcceptsGeneratedStreamViaSweet`、`TestSweetClientDiscoversNativeServer`（`interoptest/`） | stock `grpc.health.v1.Health` 的一元与生成式流方法都通，两个方向都测 |
| 内置 gRPC 反射与健康服务 | `TestNativeClientCallsSweetServer`、`TestReflectionListsRegisteredService` | 见下一节的 grpcurl 外部实测 |

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
ok  	github.com/javahongxi/sweet/interoptest	(cached)
ok  	github.com/javahongxi/sweet/nacos	(cached)
ok  	github.com/javahongxi/sweet/registry/nacos	(cached)
ok  	github.com/javahongxi/sweet/sweettest	(cached)
```

（原文照抄，包名与结果之间是制表符；最后一列是耗时，本机这次命中了测试缓存所以显示 `(cached)`，
首次跑会是秒数。）

连真实 Nacos 的测试用环境变量打开（不设就 skip，不会假装跑过）：

```
NACOS_GRPC_ADDR=127.0.0.1:9848 go test ./nacos ./registry/nacos ./interoptest -count=1
```

## 版本

没有语义化 tag，`go get` 拿到的是 pseudo-version，例如
`v0.0.0-20261027193012-b3fc07f6887d`；`@latest` 也解析到默认分支的这个 pseudo-version（实测）。
要长期钉住就写 `@<commit>`。升级 `grpc-go` 之后把上面两条命令都跑一遍——离那条表里的主张近，
别只跑离线的那条。

&copy; [hongxi.org](http://hongxi.org)
