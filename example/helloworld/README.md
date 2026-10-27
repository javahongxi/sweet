# helloworld（手写 `grpc.ServiceDesc`）

两个终端即可跑通 sweet 的端到端：服务端上线、客户端按服务名寻址并调用**四种 gRPC 方法形态**。
全程不需要 Nacos，也不需要 protoc —— 服务用 `grpc.ServiceDesc` 手写、消息用
`wrapperspb.StringValue`。

| 形态 | 方法 | 入 → 出 |
| --- | --- | --- |
| 一元 | `Ping` | 一个请求 → 一个 `pong` |
| 服务端流 | `Events` | 一个 CSV 请求 → 每个 token 一帧 |
| 双向流 | `Echo` | 每帧一个值 → 每帧回一个 `pong(值)` |
| 客户端流 | `Ack` | 每帧一个值 → 半关闭后一帧 `pong(全部)` |

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

`-values a,b,c` 是三种流式共用的一串负载（`Events` 拿整个 CSV 让服务端切，`Echo`/`Ack`
一个值一帧）。

实测（2026-10-28，direct 那发，删去每行 Go log 的 `YYYY/MM/DD HH:MM:SS ` 前缀，其余原文）：

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
Echo       <= 1 "pong(a)"
Echo       <= 2 "pong(b)"
Echo       <= 3 "pong(c)"
Echo       == 3 frames from 3 requests, closed by server
Ack        <= "pong(a,b,c)"
Ack        == 1 frame from 3 requests
```

nacos 那一发除了首两行（`resolved ... through nacos registry`、服务端 `registry=nacos`）之外，
逐帧与收尾行完全相同，不再重贴。

## 输出为什么长这样

每收到一帧打一行（`<=` 带帧号），`==` 那行只在服务端**正常关闭**流之后出现。这跟
`example/greeter/cmd/client` 逐字同形，因为两个例子的差别必须只有一个：手写桩 vs 生成桩。
调用顺序也对齐（一元 → 服务端流 → 双向流 → 客户端流），四行 `==` 收尾可以一一对着看。

收取循环仍然藏在 `ping.*` 方法里（回调式交付），不在调用方重写一遍 `Recv`——那是本例相对
生成码那份要演示的东西，不能被"顺手改清楚"抹掉。`Ack` 是唯一没有 `io.EOF` 可等的：它只有一
帧回复，所以流在没有回复时被关掉要当作失败（生成桩里是把 `io.EOF` 改写成
`io.ErrUnexpectedEOF`，这里等价地返回一个错误）。

`-values x` 时收尾行跟着变单数（`Events == 1 frame, closed by server`、
`Echo == 1 frame from 1 request, closed by server`），实测于同一端口。

nacos 路径下 `-advertise` 不再需要：实例自己上报 ip:port，客户端只认服务名。
direct 注册表不做发现（`Watch` 只返回给定端点一次、`Expose` 是记账 no-op），所以
它才需要 `-advertise`。
