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

`interoptest/` 验证 stock grpc-go 客户端与服务端在两个方向上的互通，包括一元、生成式流方法、反射、健康检查和原生重试配置。具体测试与实测读数见[测试与能力验证](testing.md)。

跨语言示例：

- [jaws-wire](../example/jaws-wire/README.md)：sweet 的 Go 客户端经 direct 或 Nacos 调用 jaws 的 Java gRPC 服务端。
- jaws 仓库的 `jaws-samples/jaws-sample-wire-sweet`：jaws 的 wire 客户端经 Nacos 发现并调用 sweet 启动的 grpc-go 服务。
