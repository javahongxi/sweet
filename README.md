# sweet

基于 gRPC 的 Go RPC 框架，服务注册与发现对接 Nacos，传输与线格式由 grpc-go 承担。
框架侧手写的是注册协议、实例视图与生命周期；grpc-go 承担 HTTP/2、gRPC 帧、protobuf 编解码与四种调用模型。

sweet 不重新发明 RPC 线格式：需要跨语言互通时用 gRPC 本身就是答案。它解决的问题是「一个 Go 服务如何上线、被找到、被调通、下线不丢请求」，而这半件事在 Go 生态里没有主流实现。

## 对外提供什么

```go
reg, err := nacos.New(nacos.Options{ServerAddr: "127.0.0.1:9848"})   // reg.Name() == "nacos"
_ = err                                                              // (error handling elided for brevity)

srv, err := server.New(server.Options{
    Address:     "127.0.0.1:0",     // ":0" 由系统分配端口，注册时上报实际端口
    ServiceName: "greeter.Greeter", // 留空则取 Register 时 ServiceDesc 的名字
    Weight:      100,               // 仅随实例透传给对端，本客户端不据此分流量
    Metadata:    map[string]string{"group": "DEFAULT_GROUP", "version": "1.0.0"},
    Registry:    reg,
    DrainDelay:  200 * time.Millisecond,
})
defer srv.Stop() // 注销 → 等 drain → GracefulStop

if err := srv.Register(&pb.GreeterServiceDesc, impl); err != nil { ... }

go func() { _ = srv.Serve() }()   // 先注册进 registry，再开始 accept
if err := srv.Listen(); err != nil { ... } // 同步点：返回 nil 后 Addr()/Port() 才可读

conn, err := client.Dial(reg, "greeter.Greeter",
    grpc.WithTransportCredentials(insecure.NewCredentials()))
cli := pb.NewGreeterClient(conn)
```

服务名只有一套：`Register` 时取 `grpc.ServiceDesc.ServiceName`，也就是生成码里的 `<package>.<Service>`，框架不再引入第二套标识。寻址目标是 `"<registry.Name()>:///<serviceName>"`，由 `Dial` 拼出，不需要手写 URI。

`registry/nacos` 已可用：命名协议（注册 / 注销 / 查询 / 订阅 / 推送 + ACK）全部手写，不依赖 nacos-sdk-go。`registry/direct` 是不接注册中心时的直通配置，同时作 nacos 实现的测试替身。

## 手写 / 用库的边界

| 组成 | 归属 | 内容 |
|---|---|---|
| 传输与调用模型 | **用库**（`grpc-go`） | HTTP/2、SETTINGS/流控/HEADERS/DATA/trailers、unary 与三种 stream、codec、interceptor |
| 服务端封装 | 自写（薄） | `Serve`/`Stop` 生命周期、监听端口回填、注销与 drain 顺序 |
| 客户端封装 | 自写（薄） | 拼 target（scheme + 服务名）、按连接注入 resolver builder（不注册全局） |
| `registry` 抽象 | 自写 | 一个接口（`Name`/`Expose`/`Watch`）+ 两个实现：`nacos`（手写协议）、`direct`（直通，兼测试替身） |
| **Nacos 客户端协议** | **自写（主战场）** | `Payload{Metadata{type,headers}, body}` 信封、bidi 常连与 `ConnectionSetupRequest` 握手（含能力协商）、保活与重连状态机、推送接收与 `requestReply` ACK、注册/注销/查询/订阅 |
| 实例视图 | 自写 | registry 内部：每个服务名一个带缓冲 channel，推送单点分发；订阅响应自带初始快照，无需再查一次。**没有独立的快照数据结构**（无 `view` 包、无 `atomic.Pointer` 换表）——一次只服务一个订阅者的 channel 就是当前的视图层 |
| 寻址咬合 | 自写（薄） | `resolver.Builder`/`State`/`ClientConn.UpdateState`；weight 与 metadata 装进 `resolver.Address.Attributes` 交给 balancer |
| 负载均衡策略 | **用库**（`grpc-go`） | 就是 `round_robin`，等权轮询。**不读 weight**，也没有加权/预热/最小活跃任何一种策略 |

即「客户端负载均衡」目前只做了一半：发现与地址交接是手写的，**选谁**完全交给库。`Weight` 是透传字段——对端（例如按 weight 做 warmup 的 Java 客户端）能读到它，sweet 自己读到也不改变任何分流。要做加权需要自定义 `balancer.Builder`，目前没有，也不在计划里。
