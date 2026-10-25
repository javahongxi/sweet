# helloworld（direct 注册表）

两个终端即可跑通 sweet 的端到端：服务端上线、客户端按服务名寻址并调用一元与
server-streaming 方法。全程不需要 Nacos，也不需要 protoc —— 服务用
`grpc.ServiceDesc` 手写、消息用 `wrapperspb.StringValue`。

```
# 终端 1
go run ./example/helloworld/cmd/server -listen 127.0.0.1:20000

# 终端 2
go run ./example/helloworld/cmd/client -endpoints 127.0.0.1:20000
```

预期：

```
# server
sweet server up: listen=127.0.0.1:20000 advertised=127.0.0.1:20000 service=sweet.example.v1.Ping registry=direct

# client
resolved sweet.example.v1.Ping through direct registry (127.0.0.1:20000) → Ping="pong" Events=[a b c]
```

`-registry=nacos` 会明确报错而不是假装可用：`registry/nacos` 正是本项目要手写的部分。

direct 注册表不做发现：`Watch` 只返回构造时给定的固定端点一次，`Expose` 是记账用的
no-op（它的作用是让服务端能在没有注册中心的情况下起得来）。因此服务端需要
`-advertise` 显式告诉客户端该连哪个地址；换成 nacos 之后这个参数会消失。
