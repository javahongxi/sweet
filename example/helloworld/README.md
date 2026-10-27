# helloworld（direct 注册表）

两个终端即可跑通 sweet 的端到端：服务端上线、客户端按服务名寻址并调用一元与
server-streaming 方法。全程不需要 Nacos，也不需要 protoc —— 服务用
`grpc.ServiceDesc` 手写、消息用 `wrapperspb.StringValue`。

```
# 终端 1
go run ./example/helloworld/cmd/server -registry=nacos -server 127.0.0.1:9848 -listen 127.0.0.1:20000

# 终端 2
go run ./example/helloworld/cmd/client -registry=nacos -server 127.0.0.1:9848
```

不接注册中心时走 direct（客户端要自己知道地址）：

```
go run ./example/helloworld/cmd/server -registry=direct -listen 127.0.0.1:20000
go run ./example/helloworld/cmd/client -registry=direct -endpoints 127.0.0.1:20000
```

实测（2026-10-28，删去每行 Go log 的 `YYYY/MM/DD HH:MM:SS ` 前缀，其余原文）：

```
# server
registry=direct is a passthrough: clients should dial -endpoints 127.0.0.1:20000
sweet server up: listen=127.0.0.1:20000 advertised=127.0.0.1:20000 service=sweet.example.v1.Ping registry=direct

# client
resolved sweet.example.v1.Ping through direct registry
Ping       <= "pong"
Events     <= 1 "a"
Events     <= 2 "b"
Events     <= 3 "c"
Events     == 3 frames, closed by server
```

`Events` 逐帧打印，格式与 `example/greeter/cmd/client` 一致；收取循环仍然藏在
`ping.Events` 里（这正是本例与生成码那份的对照点），只是交付从"收完再返回切片"改成
"到一帧回调一次"。

nacos 路径下 `-advertise` 不再需要：实例自己上报 ip:port，客户端只认服务名。
direct 注册表不做发现（`Watch` 只返回给定端点一次、`Expose` 是记账 no-op），所以
它才需要 `-advertise`。
