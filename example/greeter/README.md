# 生成码 Go 调 Go：example/greeter

`example/helloworld` 两侧都是**手写**的 `grpc.ServiceDesc`，消息用 `wrapperspb.StringValue`；
`example/jaws-wire` 用的是生成码，但对端是 Java、只有客户端侧。缺的那一半是：
**自己写 proto → protoc 生成 → 生成的 `Greeter_ServiceDesc` 挂上 sweet server、生成的
`NewGreeterClient` 通过 sweet 拨号**，两端都是本仓库的 Go 代码。顶层 README「用法」里那两段
片段（`srv.Register(&pb.Greeter_ServiceDesc, impl)` 与 `pb.NewGreeterClient(conn)`）到这一篇
才有跑得通的实现。

## 四种方法形态，前两种与 helloworld 逐行同形

`Greet`（一元）和 `GreetStream`（server-streaming）刻意跟 `example/helloworld` 一一对应，
flags 同名同义，只有默认端口不同（这里 20001，helloworld 20000），所以这两发可以直接 diff，
也能同时跑（两个 server 各占自己的默认端口，两个 client 各自解析成功）。`GreetAll`
（client-streaming）与 `GreetChat`（bidi）是这一篇才有的另外两种形态：手写那一侧要为它们自己
拼 `NewStream` 和方法名字符串，正是这一篇要避开的东西。

| | helloworld | greeter |
| --- | --- | --- |
| 消息 | `wrapperspb.StringValue` | `GreetRequest` / `GreetReply`（proto 里定义，`times` 是 int32）|
| 服务端注册 | 手写 `ping.ServiceDesc` | `&pb.Greeter_ServiceDesc`（生成）|
| 客户端 | 手写 `ping.NewClient`，`Invoke`/`NewStream` 里方法名是字符串拼接 | `pb.NewGreeterClient`（生成，全方法名常量在桩里）|
| 服务名 | 常量 `ping.ServiceName` | `pb.Greeter_ServiceDesc.ServiceName`，来自 proto 的 `package` + `service` |
| 服务端流类型 | `grpc.ServerStreamingServer[wrapperspb.StringValue]` | `pb.Greeter_GreetStreamServer`（生成的别名，同一个类型）|
| 流的收取循环 | 藏在 `ping.Events` 里 | 调用侧自己 `Recv` 到 `io.EOF` |
| 客户端流 | 没有 | 客户端 `CloseAndRecv` 收口；服务端从 `Recv` 的 `io.EOF` 才知道输入完了 |
| 双向流 | 没有 | 生成的是 `grpc.BidiStreamingClient/Server` 别名，每帧边到边答 |

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
resolved sweet.example.v1.Greeter through direct registry
Greet       <= "Hello, sweet!"
GreetStream <= 1 "Hello #1, sweet!"
GreetStream <= 2 "Hello #2, sweet!"
GreetStream <= 3 "Hello #3, sweet!"
GreetStream == 3 frames, closed by server
GreetChat   <= 1 "Hello, a!"
GreetChat   <= 2 "Hello, b!"
GreetChat   <= 3 "Hello, c!"
GreetChat   == 3 frames from 3 requests, closed by server
GreetAll    <= "Hello, a, b, c!"
GreetAll    == 1 frame from 3 requests
```

流式方法**每收到一帧打一行**（`<=` 带帧号），`==` 那行是流被服务端正常关闭（`io.EOF`）后的收尾计数。
把整条流收进切片再一次打印，就把"这是流"这件事从输出里抹掉了：看不出帧是陆续到的，也看不出流中途
断掉时已经落了几帧——下面 `-names "a,,c"` 那发的 `after 1 frames` 就是这个计数在起作用。

一元与服务端流的入参是 `-name` / `-times`，两种流式的入参是 `-names`（一个名字一帧）。
`-times 1` 时流里只有一帧，收尾行也跟着变单数：

```
GreetStream <= 1 "Hello #1, sweet!"
GreetStream == 1 frame, closed by server
```

`-names alice,bob` 发两帧，双向流答两条、客户端流合成一条：

```
GreetChat   <= 1 "Hello, alice!"
GreetChat   <= 2 "Hello, bob!"
GreetChat   == 2 frames from 2 requests, closed by server
GreetAll    <= "Hello, alice, bob!"
GreetAll    == 1 frame from 2 requests
```

`-names ""` 一帧都不发，两种语义在这里分道：双向流没收到名字就不答（不算错），客户端流
承诺了一个回复，零帧是 `InvalidArgument`：

```
GreetChat   == 0 frames from 0 requests, closed by server
GreetAll CloseAndRecv: rpc error: code = InvalidArgument desc = at least one name is required
```

`-names "a,,c"` 的空名先被双向流撞掉，进程随即退出，这一发没轮到客户端流。`(after 1 frame)`
就是逐帧计数的用处——一眼看出流是在第几帧上断的：

```
GreetChat   <= 1 "Hello, a!"
GreetChat Recv: rpc error: code = InvalidArgument desc = name is required (after 1 frame)
```

一元的 `-name ""` 打到的是同一个 `checkName`（四个入口共用一处校验，见下文
「每帧校验只有一处」）：

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
resolved sweet.example.v1.Greeter through nacos registry
Greet       <= "Hello, sweet!"
GreetStream <= 1 "Hello #1, sweet!"
GreetStream <= 2 "Hello #2, sweet!"
GreetStream <= 3 "Hello #3, sweet!"
GreetStream == 3 frames, closed by server
GreetChat   <= 1 "Hello, a!"
GreetChat   <= 2 "Hello, b!"
GreetChat   <= 3 "Hello, c!"
GreetChat   == 3 frames from 3 requests, closed by server
GreetAll    <= "Hello, a, b, c!"
GreetAll    == 1 frame from 3 requests
```

客户端这几行只证明「解析到了一个地址并能调用」，不证明注册名就是生成码那个名字，所以另查一次
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

## 每帧校验只有一处

四个入口的「名字不能为空」是同一段代码（`cmd/server/main.go`）：

```go
func checkName(name string) error {
	if name == "" {
		return status.Error(codes.InvalidArgument, "name is required")
	}
	return nil
}
```

`Greet`、`GreetStream`、`GreetChat`、`GreetAll` 各自在入口调它，所以上面 `-name ""`（一元）与
`-names "a,,c"`（双向流）打到的是同一条分支，只是从两个入口进去。客户端流另有一条只属于它的
规则：它承诺了一个回复，所以零帧是 `at least one name is required`；双向流没有这个承诺，
零帧就是零答，不算错。

## 漏写一个方法会怎样

`pb.GreeterServer` 接口里有一个不可导出的 `mustEmbedUnimplementedGreeterServer()`，只能靠内嵌
`pb.UnimplementedGreeterServer` 满足；而那个内嵌件**自带全部四个方法**。代价实测如下（每次都是
把 `cmd/server/main.go` 临时改坏、产物写到仓库外、跑完逐字节恢复并 `cmp` 确认）：

- **方法名写错**（等价于忘了实现这个流式方法）：`go build` 退出码 0、stderr 零字节，接口照样
  满足——`GreetChat` 由内嵌件顶上。信号要到调用时才出现，服务端全程没有一句提示：

```
GreetChat Recv: rpc error: code = Unimplemented desc = method GreetChat not implemented (after 0 frames)
```

- **签名写错**（多一个参数）：`var _ pb.GreeterServer = greeter{}` 让它在 build 期就炸，
  并且点名是哪个方法：

```
example/greeter/cmd/server/main.go:53:26: cannot use greeter{} (value of struct type greeter) as greeterpb.GreeterServer value in variable declaration: greeter does not implement greeterpb.GreeterServer (wrong type for method GreetChat)
		have GreetChat(greeterpb.Greeter_GreetChatServer, int) error
		want GreetChat(grpc.BidiStreamingServer[greeterpb.GreetRequest, greeterpb.GreetReply]) error
```

这段 have/want 顺带证明了 `pb.Greeter_GreetChatServer` 与
`grpc.BidiStreamingServer[GreetRequest, GreetReply]` 是同一个类型，只是生成出来的别名。

那行断言为什么值得写：sweet 的 `Register` 收的是 `*grpc.ServiceDesc` 加 `any`，不经过生成的
`RegisterGreeterServer`（`server/server.go:87`）；而 grpc-go v1.84.0 的 `Server.RegisterService`
对 handler 类型不匹配是直接 `grpclog.Fatalf`，且它没有返回值可检查
（`grpc-go@v1.84.0/server.go:778-787`——这一条是读源码得到的依据，不是这一篇实测的）。少了断言，
签名错误要到进程被终止才看得见。

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
