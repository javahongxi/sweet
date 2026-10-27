# 生成码 Go 调 Go：example/greeter

`example/helloworld` 两侧都是**手写**的 `grpc.ServiceDesc`，消息用 `wrapperspb.StringValue`；
`example/jaws-wire` 用的是生成码，但对端是 Java、只有客户端侧。缺的那一半是：
**自己写 proto → protoc 生成 → 生成的 `Greeter_ServiceDesc` 挂上 sweet server、生成的
`NewGreeterClient` 通过 sweet 拨号**，两端都是本仓库的 Go 代码。顶层 README「用法」里那两段
片段（`srv.Register(&pb.Greeter_ServiceDesc, impl)` 与 `pb.NewGreeterClient(conn)`）到这一篇
才有跑得通的实现。

## 与 helloworld 只有一处变量

方法形态刻意一一对应（一个一元、一个 server-streaming），flags 同名同义，只有默认端口不同
（这里 20001，helloworld 20000），所以两份代码可以直接 diff，也可以同时跑（两个 server 各占自己
的默认端口，两个 client 各自解析成功）：

| | helloworld | greeter |
| --- | --- | --- |
| 消息 | `wrapperspb.StringValue` | `GreetRequest` / `GreetReply`（proto 里定义，`times` 是 int32）|
| 服务端注册 | 手写 `ping.ServiceDesc` | `&pb.Greeter_ServiceDesc`（生成）|
| 客户端 | 手写 `ping.NewClient`，`Invoke`/`NewStream` 里方法名是字符串拼接 | `pb.NewGreeterClient`（生成，全方法名常量在桩里）|
| 服务名 | 常量 `ping.ServiceName` | `pb.Greeter_ServiceDesc.ServiceName`，来自 proto 的 `package` + `service` |
| 服务端流类型 | `grpc.ServerStreamingServer[wrapperspb.StringValue]` | `pb.Greeter_GreetStreamServer`（生成的别名，同一个类型）|
| 流的收取循环 | 藏在 `ping.Events` 里 | 调用侧自己 `Recv` 到 `io.EOF` |

服务名只有一套：`sweet.example.v1.Greeter` 是 `greeter.proto` 的 `package sweet.example.v1`
与 `service Greeter` 拼出来的，示例里没有第二套标识；`server.Options.ServiceName` 的默认值
直接取 `pb.Greeter_ServiceDesc.ServiceName`，不是另抄一个字符串。

## 跑（direct，不需要 Nacos）

```
go run ./example/greeter/cmd/server -registry=direct -listen 127.0.0.1:20001
go run ./example/greeter/cmd/client -registry=direct -endpoints 127.0.0.1:20001
```

实测（2026-10-28，只删去 Go log 的日期时间前缀，其余原文）：

```
# server
registry=direct is a passthrough: clients should dial -endpoints 127.0.0.1:20001
sweet server up: listen=127.0.0.1:20001 advertised=127.0.0.1:20001 service=sweet.example.v1.Greeter registry=direct

# client
resolved sweet.example.v1.Greeter through direct registry → Greet="Hello, sweet!" GreetStream=[Hello #1, sweet! Hello #2, sweet! Hello #3, sweet!]
```

一元与流的入参不同（`-name` / `-times`），`-times 1` 就只收到一条：

```
resolved sweet.example.v1.Greeter through direct registry → Greet="Hello, sweet!" GreetStream=[Hello #1, sweet!]
```

服务端返回类型化状态码也是生成码那套接口的一部分——`-name ""` 撞进实现里的
`status.Error(codes.InvalidArgument, "name is required")`：

```
Greet: rpc error: code = InvalidArgument desc = name is required
```

## 跑（nacos）

```
go run ./example/greeter/cmd/server -registry=nacos -server 127.0.0.1:9848 -listen 127.0.0.1:20002
go run ./example/greeter/cmd/client -registry=nacos -server 127.0.0.1:9848
```

实测：

```
# server
sweet server up: listen=127.0.0.1:20002 advertised=127.0.0.1:20002 service=sweet.example.v1.Greeter registry=nacos

# client
resolved sweet.example.v1.Greeter through nacos registry → Greet="Hello, sweet!" GreetStream=[Hello #1, sweet! Hello #2, sweet! Hello #3, sweet!]
```

客户端这一行只证明「解析到了一个地址并能调用」，不证明注册名就是生成码那个名字，所以另查一次
Nacos 自己的管理面（HTTP 8848，与客户端走的 gRPC 9848 是两条面）：

```
curl -s "http://127.0.0.1:8848/nacos/v3/admin/ns/instance/list?serviceName=sweet.example.v1.Greeter&groupName=DEFAULT_GROUP&namespaceId=public"
{"code":0,"message":"success","data":[{"instanceId":"127.0.0.1#20002#DEFAULT#DEFAULT_GROUP@@sweet.example.v1.Greeter","ip":"127.0.0.1","port":20002,"weight":100.0,"healthy":true,"enabled":true,"ephemeral":true,"clusterName":"DEFAULT","serviceName":"DEFAULT_GROUP@@sweet.example.v1.Greeter","metadata":{},"instanceHeartBeatInterval":5000,"instanceHeartBeatTimeOut":15000,"ipDeleteTimeout":30000,"instanceIdGenerator":"simple"}]}
```

`SIGTERM` 掉服务端，先注销再停止 accept：

```
signal received, draining (grace period 200ms)
stopped: deregistered then drained
```

此时同一句 curl 返回空列表：

```
{"code":0,"message":"success","data":[]}
```

再拨一次，客户端拿不到地址：

```
Greet: rpc error: code = Unavailable desc = name resolver error: produced zero addresses
```

## 重新生成

`greeterpb/doc.go` 里写成了 `go:generate`，就地可跑，不需要手敲 protoc：

```
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go generate ./example/greeter/greeterpb
```

插件是**代码生成期的工具**，不引入运行时依赖——产物只 import `google.golang.org/grpc` 与
`google.golang.org/protobuf`，都在现有依赖里。配方沿用了 jaws-wire 那边踩过的两个坑：
pragma 里插件路径写 `$HOME/go/bin/...` 绝对形式并包在 `sh -c` 里（protoc 按它自己的 PATH 找
可执行文件，而 `go:generate` 不展开变量），以及 `paths=source_relative` 保持产物平铺在本目录。

生成管线做过破坏性自证：把 `greeter.pb.go` 与 `greeter_grpc.pb.go` 移出磁盘后重跑
`go generate`，`cmp` 两份产物与原件逐字节一致。
