# sweet

基于 gRPC 的 Go RPC 框架，手写 Nacos 服务注册与发现，不依赖 nacos-sdk-go；传输与线格式使用 `grpc-go`，支持跨语言互通；内置 HTTP-to-gRPC 网关，可从 Nacos 动态加载路由并自动发现后端实例。

## 特性

- **标准 gRPC** — 直接使用生成桩，支持一元及三种流式调用，内置反射与健康服务，兼容 grpcurl
- **手写 Nacos 协议** — Naming 支持注册、查询、订阅与实例推送；Config 支持只读查询、监听与变更回拉，均不依赖 nacos-sdk-go
- **Nacos 连接管理** — Naming 与 Config 共用保活探针、死流检测和断线重连，重连后自动重放注册、订阅与配置监听
- **优雅上下线** — 先注销，再等待在途请求，最后停止服务；健康状态随生命周期变化
- **可选快照回落** — Nacos 冷启动不可达时可使用本地实例快照，默认关闭，恢复后自动回到服务端视图
- **两种注册模式** — `nacos` 提供服务发现，`direct` 用于直通、测试与无注册中心场景
- **动态 HTTP 网关** — HTTP-to-gRPC 转码，从 Nacos 加载路由并自动发现后端实例
- **互通兼容** — 已验证 Go 客户端调用 Java jaws 服务，以及标准 grpc-go 双向互通

## 快速开始

### 环境要求

- Go ≥ 1.27
- Nacos 3.x，目前只在 3.2.3 上验证；连接地址是 gRPC 端口 `9848 = 8848 + 1000`，不是 HTTP 8848
- 唯一第三方依赖是 `google.golang.org/grpc`
- 仅运行 direct 示例时不需要 Nacos

### 安装

```bash
go get github.com/javahongxi/sweet   # 尚无 tag，安装 pseudo-version
go mod tidy
```

### 运行示例

先编译 helloworld 的服务端与客户端：

```bash
go build -o /tmp/sweet-server ./example/helloworld/cmd/server
go build -o /tmp/sweet-client ./example/helloworld/cmd/client
```

direct 模式不需要 Nacos，客户端直接指定地址：

```bash
/tmp/sweet-server -registry=direct -listen 127.0.0.1:20000 &
/tmp/sweet-client -registry=direct -endpoints 127.0.0.1:20000
```

接入 Nacos 后，客户端只需服务名：

```bash
/tmp/sweet-server -registry=nacos -server 127.0.0.1:9848 -listen 127.0.0.1:20000 &
/tmp/sweet-client -registry=nacos -server 127.0.0.1:9848
```

完整的四种调用形态与优雅下线输出见 [helloworld 示例](example/helloworld/README.md)；Go 调 Java 的跨语言实测见 [jaws-wire 示例](example/jaws-wire/README.md)。

## 代码示例

### 服务端

```go
srv, err := server.New(server.Options{
    Address:     "127.0.0.1:0", // 注册时上报系统实际分配的端口
    ServiceName: "greeter.Greeter",
    Weight:      100,
    Metadata:    map[string]string{"version": "1.0.0"},
    Registry:    reg,
    DrainDelay:  200 * time.Millisecond,
})

srv.Register(&pb.Greeter_ServiceDesc, impl)
go func() { _ = srv.Serve() }() // Serve 阻塞，直到 Stop 才返回
if err := srv.Listen(); err != nil { ... }
defer srv.Stop() // 注销 → 等 drain → GracefulStop
```

注册发生在开始 accept 之前，这一窗口的行为与设计取舍见[服务发布顺序](doc/service-publish-order.md)。

### 客户端

```go
conn, err := client.Dial(reg, "greeter.Greeter",
    grpc.WithTransportCredentials(insecure.NewCredentials()))
cli := pb.NewGreeterClient(conn)
```

`Dial` 自动拼接目标串并按连接注入 resolver，不注册进程全局状态，因此一个进程可以并存多套注册中心。deadline 与取消使用 `context`，dial option、call option 与 keepalive 仍走 grpc-go 原生配置。

### 注册中心

```go
type Registry interface {
    Name() string
    Expose(ctx context.Context, serviceName string, in Instance) (func(), error)
    Watch(ctx context.Context, serviceName string) (<-chan []Instance, error)
}
```

`registry/nacos` 提供 Nacos 实现，`registry/direct` 提供直通实现。`Expose` 返回注销闭包；`Watch` 返回实例快照通道，首个快照直接来自 Nacos 订阅响应。

## 测试

```bash
go test ./...
```

默认测试离线运行，使用进程内协议桩。真实 Nacos 验证、长时测试、能力证据矩阵与升级检查见[测试与能力验证](doc/testing.md)。

## 版本

目前没有语义化 tag，`go get` 得到 pseudo-version；长期使用时建议钉到 `@<commit>`。编译只包含实际 import 的包，但 `go.mod` 的依赖图由全仓共享，详细说明与验证命令见[测试与能力验证](doc/testing.md#版本与依赖验证)。

## 深入了解更多

| 主题 | 说明 |
| --- | --- |
| [测试与能力验证](doc/testing.md) | 离线/真机测试、能力证据矩阵、版本与依赖验证 |
| [Nacos 快照回落](doc/nacos-snapshot.md) | 默认关闭的冷启动回落、恢复语义与官方客户端对照 |
| [gRPC 兼容与标准工具](doc/grpc-interop.md) | grpcurl、反射、健康状态与跨语言互通 |
| [Nacos 协议加固](doc/nacos-sdk-hardening.md) | 手写协议的生命周期、鉴权、重连与推送语义 |
| [服务发布顺序](doc/service-publish-order.md) | 为什么注册发生在 accept 之前 |
| [Nacos 网关路由](doc/nacos-config-routes.md) | 配置域客户端、动态路由格式与验证结果 |
| [grpc-gateway 适配](doc/grpc-gateway-adaptation.md) | 转码协议选择、功能调研与后续路线 |
| [helloworld](example/helloworld/README.md) | 手写 `grpc.ServiceDesc`，跑通四种调用形态 |
| [greeter](example/greeter/README.md) | proto 生成码的 Go 调 Go 完整示例 |
| [jaws-wire](example/jaws-wire/README.md) | Go 客户端调用 Java jaws 服务端 |
| [gateway](example/gateway/README.md) | HTTP-to-gRPC 网关运行示例 |

&copy; [hongxi.org](http://hongxi.org)
