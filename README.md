# sweet

基于 gRPC 的 Go RPC 框架，服务注册与发现对接 Nacos，传输与线格式由 grpc-go 承担。
框架侧手写的是注册协议、实例视图与生命周期；grpc-go 承担 HTTP/2、gRPC 帧、protobuf 编解码与四种调用模型。

sweet 不重新发明 RPC 线格式：需要跨语言互通时用 gRPC 本身就是答案。它解决的问题是「一个 Go 服务如何上线、被找到、被调通、下线不丢请求」，而这半件事在 Go 生态里没有主流实现。

## 对外提供什么

```go
// 服务端：注册实现 + 上线 + 优雅下线
srv := server.New(server.Options{Registry: "nacos", Endpoint: "127.0.0.1:9848", ServiceName: "greeter.Greeter"})
pb.RegisterGreeterServer(srv.GRPCServer(), impl)
srv.Serve()
defer srv.Stop()   // drain → 注销 → 等连接排空

// 客户端：按服务名寻址
conn := client.Dial("nacos://public/greeter.Greeter?weight=100")
cli := pb.NewGreeterClient(conn)
```

服务名取自 gRPC 自身的 `grpc.ServiceInfo.ServiceName()`，不需要额外声明一套服务标识。

## 手写 / 用库的边界

| 组成 | 归属 | 内容 |
|---|---|---|
| 传输与调用模型 | **用库**（`grpc-go`） | HTTP/2、SETTINGS/流控/HEADERS/DATA/trailers、unary 与三种 stream、codec、interceptor |
| 服务端封装 | 自写（薄） | `Serve`/`Stop` 生命周期、监听端口回填、注销与 drain 顺序 |
| 客户端封装 | 自写（薄） | `Dial` 目标解析、与 resolver/balancer 的接线 |
| `registry` 抽象 | 自写 | 一个接口 + 两个实现：`nacos`（真）与 `direct`（直连，兼作集成测试替身） |
| **Nacos 客户端协议** | **自写（主战场）** | `Payload{Metadata{type,headers}, body}` 信封、bidi 常连与 `ConnectionSetupRequest` 握手（含能力协商）、保活与重连状态机、推送接收与 `requestReply` ACK、注册/注销/查询/订阅 |
| 实例视图 | 自写 | `atomic.Pointer[map]` 快照换、单写 goroutine 消费推送、done-chan 桥接 `context` |
| 寻址咬合 | 自写（薄） | `resolver.Builder`/`State`/`ClientConn.UpdateState`；权重等属性经 `resolver.Address.Attributes` 交给 balancer |
