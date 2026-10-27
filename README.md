# sweet

基于 gRPC 的 Go RPC 框架，用 Nacos 做注册中心，手写服务注册与发现，不依赖 nacos-sdk-go。

传输与线格式一律用 `grpc-go`：sweet 不重新发明 RPC 线格式，需要跨语言互通时直接用 gRPC。

## 状态

已完成：注册 / 注销 / 查询 / 订阅 + 推送 + ACK、优雅上下线、连接断开后的重连与重放、保活探针与死流检测。

## 要求与兼容

- Go ≥ 1.27（`go.mod` 声明 `go 1.27.3`）
- 唯一第三方依赖：`google.golang.org/grpc v1.84.0`（`google.golang.org/protobuf` 是它自身的依赖，不算第二个库）
- Nacos 3.x。ephemeral 实例的注册、查询、订阅、推送**全程走 gRPC**
- 连接地址是 SDK gRPC 口：`9848 = mainPort(8848) + 1000`，不是 HTTP 的 8848
- 服务端鉴权需关闭（`nacos.core.auth.enabled=false`）
- 用嵌入式 Derby 时它是单实例锁：再启一个 standalone 会报 `XSDB6`；正确停法是 `bin/shutdown.sh`，进程存活时不要删 `db.lck`

## 五分钟跑通

不需要 Nacos 时走 direct（纯直通，不做发现，客户端自己给地址）：

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

几条值得单独看的：

| 测试 | 验的东西 | 实测 |
|---|---|---|
| `TestLiveSetupHandshakeIsAcknowledged` | 真机建连握手能完成 | 拿回服务端能力表 |
| `TestLiveProbeChannelFacts` | 保活探针在一元通道有应答、在 bidi 流上无人应答 | 两条都断言 |
| `TestIdleRegistrationSurvives` | 空载时注册是否一直存在（ephemeral 活性＝那条连接） | 3 分钟 12 次探测，实例始终在 |
| `TestRollingRestartDrill` | provider 重启时消费端不中断；失败按 gRPC 状态码分桶，不按字符串 | `calls=180 failures=0`，收敛 559.5ms / 611.1ms |
| `TestGuardConvertsPanicToStreamLoss` | 长命 goroutine 里的 panic 变成一次流丢失（可重连），既不拖垮进程也不留下 `Ready()==true` 的死连接 | panic→hook 收到原因，`Ready()` 转 false，重握手后 `Epoch()≥2` |
| `TestCauseStaysClassifiableThroughWrapping` | `%w` 一路包上去之后，调用方仍能用 `errors.As` 取到 gRPC 状态码；目标必须是指向接口的指针（`*status.Status` 不实现 `error`，那种写法编译不过） | 三层包装后 Unavailable / Canceled 都取得到 |
| `TestBackendsContract/{nacos,harbor}` | 同一份客户端代码对两个不同实现的服务端都成立（表驱动子测试，缺哪个环境变量就 skip 哪个） | 两端全绿，无 harbor 分支 |
| `TestNativeClientCallsSweetServer` / `TestSweetClientDiscoversNativeServer` | 与**改不了的**原生 grpc-go 桩（stock `grpc.health.v1.Health`）互通 | 一元与生成式流方法都通 |


&copy; [hongxi.org](http://hongxi.org)