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

## 我们不做

- 不做 RPC 线格式：不写私有帧、不写 HTTP/2 帧层（`grpc-go` 已承担）。
- 不做服务治理控制面：不写路由规则、不写限流熔断降级；那是网关或 mesh 的职责。
- 不做配置中心模块，也不借用 naming 那条连接做配置长轮询。
- 不做持久实例（`ephemeral=false`）与 `BeatReactor` 路径。
- 不做鉴权：依赖服务端 `nacos.core.auth.enabled=false`（本地靶子已核实）。
- 不做 TLS 与服务端 `negotiator`。
- 不做磁盘快照容灾，v0 只在内存保活快照。
- 不做第二个注册中心后端；`registry` 接口存在是为了让 `direct` 当测试替身，不是为了可插拔一堆实现。
- 不做 `nacos.address.server.list` 端点列表拉取，用显式 `host:port`。

## 前置（本地靶子，实测）

```
Nacos 3.2.3 standalone   home /Users/hongxi/ai-infra/nacos/standalone/nacos-3.2.3
启动参数 -Dnacos.deployment.type=merged（Server API 与 Console 两个 Tomcat）
  8848 Server API (/nacos) · 8080 Console (/) · 9848 SDK gRPC = mainPort+1000 · 9849 集群 gRPC · 7848 JRaft
sweet 只连 9848。
```

Derby 是进程内嵌库，`data/derby-data` 同时只能被一个 JVM 打开，第二个实例报 `ERROR XSDB6`；只用 `bin/shutdown.sh` → `bin/startup.sh -m standalone`，进程存活时不要删 `db.lck`。
Nacos 3.x 已移除 v1/v2 HTTP 面，探活可用 `GET /nacos/v3/admin/core/state`。

## 协议事实（行号基线：本地 `~/github/nacos` @ tag `3.2.3`，commit `c843da5be`）

> 注意该 tag 内 `pom.xml` 的 `<revision>` 仍写 `3.2.2`（Nacos 该文件未随 tag 更新），`git describe --tags` 才是 3.2.3。行号以此 checkout 为准，与官方 3.2.3 发行包若有一字之差，以真帧为准。

| 事实 | 依据（3.2.3 行号） |
|---|---|
| `Metadata.type` = 请求/响应类的 **simple class name**（不是枚举、不是数字） | `common/.../remote/client/grpc/GrpcUtils.java:59,79,102` |
| 服务端按该字符串反查类；查不到即 `Unknown payload type:` | 同上 `:123,134`；`common/.../remote/PayloadRegistry.java` |
| header 键 `module`，命名模块取值 `naming` | `api/.../remote/RemoteConstants.java:36,40` |
| header 键 `clientIp` | `api/.../remote/request/RequestTest.java:38-44` |
| 服务面只有两个方法：一元 `Request.request`、bidi `BiRequestStream.requestBiStream` | `api/src/main/proto/nacos_grpc_service.proto` |
| **一元通道**：`requestBlockingStub.request(Payload)` 拿 `ListenableFuture<Payload>`，**关联由 gRPC 调用本身承担** | `common/.../grpc/GrpcClient.java:243`（`serverCheck` 即此形状） |
| **bidi 通道**：`bindRequestStream` 用 `streamStub.requestBiStream(...)`，建连时先发 `ConnectionSetupRequest` | `GrpcClient.java:262-279`（`:276` 处 `setupRequestHandler.requestReply(request, null)`） |
| 握手载荷只有四个字段：`clientVersion` / `tenant` / `labels: Map<String,String>` / `abilityTable: Map<String,Boolean>`——能力协商在建连那一刻完成 | `api/.../remote/request/ConnectionSetupRequest.java:30-36` |
| 服务端能力查询入口 | `NamingGrpcClientProxy.java:491`（`rpcClient.getConnectionAbility(abilityKey) == SUPPORTED`） |
| 收到推送必须沿同一条 bidi 流回一个 response 作为 ACK | `common/.../remote/client/RpcClient.java:859`（`serverRequestHandler.requestReply(request, currentConnection)`） |
| 命名四类请求的构造点 | `NamingGrpcClientProxy.java`：`InstanceRequest` `:280`（注册）`:343`（注销）、`ServiceQueryRequest` `:375`、`SubscribeServiceRequest` `:439`（订阅）`:473`（退订）、`BatchInstanceRequest` `:264`（v0 不做） |
| 推送载荷 `NotifySubscriberRequest` 字段：`namespace` / `serviceName` / `groupName` / `serviceInfo` | `api/.../naming/remote/request/NotifySubscriberRequest.java:31-37` |

**已推翻的一处预设**：`requestId` **不参与命名协议的请求-响应配对**。它在 `api`/`common` 主代码里只出现在 `DefaultRequestFuture`（超时与日志用途），既不是 `Metadata.headers` 的键，也不在 `SubscribeServiceRequest` 或 `NotifySubscriberRequest` 的字段里。这条原样写进了 `nacos/conn` 的设计：只需要「一条 bidi 连接 + 一元 RPC」，**不需要自建 pending map**——那部分是 grpc-go 已经在做的。

**待钉**：`Metadata` 里除 `clientIp` 外还有哪些必填键（`headers` 的完整键集）；服务端 `ConnectionReLoader`/重连后重新订阅的确切时序。这两项在 S2/S4 用真帧确认，不凭源码转述。

## 目录

```
server/     服务端封装：Serve / Stop / drain
client/     客户端封装：Dial
registry/   注册抽象；registry/nacos（走 nacos/）、registry/direct（直连，兼测试替身）
nacos/      手写协议层：conn（bidi 常连、握手、保活、重连、请求-推送配对）、api（四类请求的信封编解码）
view/       实例快照：atomic.Pointer 换表 + 单写 goroutine
resolver/   grpc-go resolver/balancer 适配
proto/      原样取自 Apache Nacos 的 .proto（见 proto/README.provenance）
gen/        由它生成的 Payload/Metadata message 类型；不生成、不使用 grpc stub
```

`google.golang.org/protobuf` 是 `grpc-go v1.84.0` 自身的依赖（其 `go.mod:29`），因此 `gen/` 不构成第二个第三方库。

## 验收

1. 两端都是 sweet：`server.New` + `client.Dial`，经 Nacos 寻址跑通 unary 与 server-streaming。
2. 注册/查询/订阅/注销四动作在真 Nacos（127.0.0.1:9848）上完成，并能收到实例变更推送。
3. **与原生 gRPC 对打**：原生 grpc-go 服务端 + 手写 stub 注册进 Nacos → sweet client 调通；反向同理。证明"用 gRPC 就当得了真 gRPC"。
4. **连 harbor**：同一份 nacos 协议代码不改，连 harbor 伪装成的 Nacos gRPC 面；harbor 未声明的能力必须显式报错或显式降级，不允许静默。
5. 优雅下线：滚动重启 provider，消费侧无请求失败、无连接风暴、`UpdateState` 次数在预期范围。
6. 并发读快照 `go test -race` 干净，并给出快照读的 `ns/op` 与 `allocs/op`。

第 3、4 条是本项目存在的核心理由：**自测全绿不等于协议互通**，验证必须从不属于你的那一侧打过来。
