# sweet

基于 gRPC 的 Go RPC 框架，用 Nacos 做服务注册与发现；Nacos 命名协议的 Go 客户端自己手写，不依赖 nacos-sdk-go。

传输与线格式一律用 `grpc-go`：sweet 不重新发明 RPC 线格式，需要跨语言互通时直接用 gRPC。sweet 只管这一层的事——一个 Go 服务如何上线、被对端找到、被调通、下线时不丢请求。

学习/演示性质，不承诺生产可用（无压测、无重试/限流、无版本兼容矩阵）。但下面每条主张都附了可当场跑的命令或测试，跑不出来就算 bug。

## 状态

已完成：注册 / 注销 / 查询 / 订阅 + 推送 + ACK、优雅上下线、连接断开后的重连与重放、保活探针与死流检测。

未做（原因见「不做什么」）：鉴权、TLS、配置中心、持久实例、加权负载均衡、磁盘快照容灾、压测与性能基线。

## 要求与兼容

- Go ≥ 1.27（`go.mod` 声明 `go 1.27.3`）
- 唯一第三方依赖：`google.golang.org/grpc v1.84.0`（`google.golang.org/protobuf` 是它自身的依赖，不算第二个库）
- Nacos 3.x。ephemeral 实例的注册、查询、订阅、推送**全程走 gRPC，没有 HTTP 命名面**（1.x 那套「HTTP 注册 + 长轮询订阅」在 2.x/3.x 已不成立）
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
resolved sweet.example.v1.Ping through direct registry → Ping="pong" Events=[a b c]
```

接上 Nacos 时走 nacos（客户端不再需要地址，只认服务名）：

```
/tmp/sweet-server -registry=nacos -server 127.0.0.1:9848 -listen 127.0.0.1:20000 &
/tmp/sweet-client -registry=nacos -server 127.0.0.1:9848
```

实测输出：

```
sweet server up: listen=127.0.0.1:20000 advertised=127.0.0.1:20000 service=sweet.example.v1.Ping registry=nacos
resolved sweet.example.v1.Ping through nacos registry → Ping="pong" Events=[a b c]
```

`Ctrl-C`（或 `kill`）掉服务端后再拨一次，能看到优雅下线的结果——实例先注销、客户端随即拿不到地址：

```
Ping: rpc error: code = Unavailable desc = name resolver error: produced zero addresses
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

srv.Register(&pb.GreeterServiceDesc, impl)   // 用生成码的 ServiceDesc，不另造服务标识
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

两个实现：`registry/nacos`（手写协议）与 `registry/direct`（直通）。`Expose` 返回注销闭包；`Watch` 首次调用即订阅，返回的第一个快照来自订阅响应本身（Nacos 的 subscribe 会带回当前实例列表，不需要额外查一次）。

## 测试

```
go test ./...                                    # 离线，用进程内协议桩
```

```
ok  github.com/javahongxi/sweet/nacos/conn
ok  github.com/javahongxi/sweet/registry/nacos
ok  github.com/javahongxi/sweet/sweettest
ok  github.com/javahongxi/sweet/interoptest
```

连真实 Nacos 的测试用环境变量打开（不设就 skip，不会假装跑过）：

```
NACOS_GRPC_ADDR=127.0.0.1:9848 go test ./nacos/conn ./registry/nacos ./interoptest -count=1
```

几条值得单独看的：

| 测试 | 验的东西 | 实测 |
|---|---|---|
| `TestLiveSetupHandshakeIsAcknowledged` | 真机建连握手能完成 | 拿回服务端能力表 |
| `TestLiveProbeChannelFacts` | 保活探针在一元通道有应答、在 bidi 流上无人应答 | 两条都断言 |
| `TestIdleRegistrationSurvives` | 空载时注册是否一直存在（ephemeral 活性＝那条连接） | 3 分钟 12 次探测，实例始终在 |
| `TestRollingRestartDrill` | provider 重启时消费端不中断 | `calls=176 failures=0`，收敛 562.6ms / 561.3ms |
| `TestNacosBackendContract` / `TestHarborBackendContract` | 同一份客户端代码对两个不同实现的服务端都成立 | 两端全绿，无 harbor 分支 |
| `TestNativeClientCallsSweetServer` / `TestSweetClientDiscoversNativeServer` | 与**改不了的**原生 grpc-go 桩（stock `grpc.health.v1.Health`）互通 | 一元与生成式流方法都通 |

## 不做什么

- 不写私有协议、不写 HTTP/2 帧层：线格式交给 `grpc-go`。需要跨语言互通时应当直接用 gRPC，而不是本项目。
- 不做服务治理控制面：路由规则、限流、熔断、降级都不是注册发现这一层的事。
- 不做加权负载均衡：sweet 自己不配负载均衡策略（只有滚动重启那个测试为了断言分布，显式设了 `round_robin`），因此没有任何读 weight 的逻辑。`Weight` 只透传给对端（例如按 weight 做 warmup 的 Java 客户端会读它），Go provider 没有 JIT 预热需求，本地不解释。
- 不做配置中心模块，也不借用 naming 那条连接做配置长轮询。
- 不做持久实例（`ephemeral=false`）与其 `BeatReactor` 路径。
- 不做鉴权、TLS，也不碰服务端的 `negotiator`。
- 不做磁盘快照容灾，v0 只在内存里保活。
- 不做 `nacos.address.server.list` 端点列表拉取，用显式 `host:port`。
- 不做第二个注册中心后端；`Registry` 接口只有两个实现，`direct` 是为「不接注册中心也能开发」和当测试替身而存在。

## 手写 / 用库的边界

设计说明，不是使用文档；放在最后。

| 组成 | 归属 | 内容 |
|---|---|---|
| 传输与调用模型 | **用库**（`grpc-go`） | HTTP/2、SETTINGS/流控/HEADERS/DATA/trailers、unary 与三种 stream、codec、interceptor |
| 服务端封装 | 自写（薄） | `Serve`/`Stop` 生命周期、监听端口回填、注销与 drain 顺序 |
| 客户端封装 | 自写（薄） | 拼 target（scheme + 服务名）、按连接注入 resolver builder（不注册全局） |
| `registry` 抽象 | 自写 | 一个接口（`Name`/`Expose`/`Watch`）+ 两个实现：`nacos`（手写协议）、`direct`（直通，兼测试替身） |
| **Nacos 客户端协议** | **自写（主战场）** | `Payload{Metadata{type,headers}, body}` 信封、bidi 常连与 `ConnectionSetupRequest` 握手（含能力协商）、保活与重连状态机、推送接收与 `requestReply` ACK、注册/注销/查询/订阅 |
| 实例视图 | 自写 | registry 内部：每个服务名一个带缓冲 channel，推送单点分发；订阅响应自带初始快照。**没有独立的快照数据结构**（无 `view` 包、无 `atomic.Pointer` 换表） |
| 寻址咬合 | 自写（薄） | `resolver.Builder`/`State`/`ClientConn.UpdateState`；weight 与 metadata 装进 `resolver.Address.Attributes` 交给 balancer |
| 负载均衡策略 | **用库**（`grpc-go`） | sweet 不设策略，由 grpc-go 决定；没有加权逻辑（只有滚动重启测试为断言分布显式设了 `round_robin`） |

即「客户端负载均衡」目前只做了一半：发现与地址交接是手写的，**选谁**完全交给库。
