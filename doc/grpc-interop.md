# gRPC 兼容与标准工具

sweet 不改写 gRPC 线格式，也不要求对端感知 sweet。生成桩、标准 grpc-go 服务与客户端、grpcurl 等工具都可以直接使用。

## grpcurl 实测

`server.New` 顺手注册了 gRPC 反射与健康服务——两者都在 `grpc-go` 自己仓库里，不构成第二个依赖。意义不在"多了两个服务"，而在**对端不需要知道 sweet 存在**：`grpcurl` 这类标准工具能自己列出实例上有什么服务、读出 schema、直接调一刀。

实测环境：2026-10-28，`grpcurl 1.9.4`，对端通过下面的命令启动：

```bash
go run ./example/greeter/cmd/server -registry=direct -listen 127.0.0.1:20001
```

列出服务：

```text
$ grpcurl -plaintext localhost:20001 list
grpc.health.v1.Health
grpc.reflection.v1.ServerReflection
grpc.reflection.v1alpha.ServerReflection
sweet.example.v1.Greeter
```

直接调用服务端流方法：

```text
$ grpcurl -plaintext -d '{"name":"grpcurl","times":2}' localhost:20001 sweet.example.v1.Greeter/GreetStream
{
  "message": "Hello #1, grpcurl!"
}
{
  "message": "Hello #2, grpcurl!"
}
```

`describe sweet.example.v1.Greeter` 会把四个 rpc 的完整 schema 打出来，含 `stream` 标注，说明列出来的不只是名字。

## 健康状态与生命周期

健康状态跟着 sweet 的生命周期走，而不是恒为 `SERVING`：`grpc-go` 的 `health.NewServer()` 一开始就把总体服务标成 SERVING，而那时还没监听，所以 `New` 里显式改成 `NOT_SERVING`，直到 `Serve` 绑好端口并 `Expose` 成功才翻成 SERVING，注册的每个服务名各自一份状态。

`Stop` 一开始就 `Shutdown()`，比注销更早一步告诉探活方"别再发流量了"。完整的服务发布与关停顺序见[服务发布顺序](service-publish-order.md)。

## 标准桩与跨语言互通

`interoptest/` 把 sweet 与 stock grpc-go 配在一起跑一元、生成式流方法、反射与健康检查。但要说清一件事：sweet 不实现线格式，RPC 面两端都是 grpc-go，所以这几条测的是**接线**——健康状态只在 `Serve` 绑好端口并 `Expose` 成功之后才翻成 SERVING、反射列得出实例真正在服的服务——而不是协议互通。真正的外部对照只有一条：`TestSweetClientDiscoversNativeServer`，发现面走的是 sweet 自己手写的 Nacos 协议，一个 stock 服务端经它被发现、再用 stock 桩调通，这才是关于 sweet 自己写的东西的证据。

原生重试配置不在 `interoptest/`，在 `sweettest/retry_test.go`：那是 stock 的 service config，sweet 没加自己的重试旋钮。具体测试与实测读数见[测试与能力验证](testing.md)。

跨语言示例：

- [jaws-wire](../example/jaws-wire/README.md)：sweet 的 Go 客户端经 direct 或 Nacos 调用 jaws 的 Java gRPC 服务端。
- jaws 仓库的 `jaws-samples/jaws-sample-wire-sweet`：jaws 的 wire 客户端经 Nacos 发现并调用 sweet 启动的 grpc-go 服务。

## 与 jaws 的命名与 group 约定

两个框架共用一套 group 默认值，框架身份靠服务名前缀表达，不靠 group：

| 维度                           | sweet                              | jaws          | 说明                                                                                          |
|--------------------------------|------------------------------------|---------------|-----------------------------------------------------------------------------------------------|
| naming group                   | `default_rpc`                      | `default_rpc` | 共用默认值；`registry/nacos` 与 `applevel` provider 的默认值即此                              |
| 服务名                         | 裸名（`sweet.example.v1.Greeter`） | `jaws/{name}` | Nacos 身份是 (tenant, group, name) 三元组，同组内前缀不同即不同服务，接口全名相同也不会混实例 |
| 应用级 mapping 的 config group | `JAWS_CONFIG`                      | `JAWS_CONFIG` | 配置域固定值（`applevel.ConfigGroup`），与 naming group 是两个独立维度，不随其变化            |

设计取舍：group 的本职是业务域/环境隔离。若让它兼任框架判别器（sweet 曾用
`DEFAULT_GROUP`、jaws 用 `default_rpc`），就与前缀的判别职责冗余，每条互通链路都要
一个 group 对齐开关——消费端按模式切 group、网关与示例显式传 `-group`。对齐之后这些
开关全部消失：jaws 消费 sweet 只需置空 `registryNamespace` 一处适配，`-applevel` 两种
模式同 group；sweet↔sweet 裸名约定只需 `-namespace=`。

边界两条：底层 `nacos/naming_client.go` 保持协议层的 `DEFAULT_GROUP` 默认（那是 Nacos
官方客户端的惯例，框架层显式传值覆盖）；网关路由表仍在配置域 `DEFAULT_GROUP`，与
naming 无关。另注意 jaws 若显式 `registryNamespace=` 注册裸名，会与同名 sweet 服务在
同组相撞——前缀兜底只在默认配置下成立。

应用级契约的完整表格（revision 算法、StringValue 信封、元数据拉取路径）见
[applevel 示例](../example/applevel/README.md)；接口级互通的实测见
[jaws-wire 示例](../example/jaws-wire/README.md)与 jaws 仓库的 `jaws-sample-wire-sweet`。
