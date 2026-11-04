# 服务发布顺序：注册在 accept 之前，以及为什么保持现状

`server.Server.Serve()` 把实例发布到注册中心这件事，发生在 gRPC 的 accept 循环**之前**。
这份文件记三样东西：次序与后果、后果的实测读数、以及 go-zero / dubbo-go / jaws 三个对照物
在这条轴上的位置。代码里的注释只留结论，分析在这里。

## 次序（`server/server.go`）

| 步 | 语句 | 行 |
|---|---|---|
| 1 | `net.Listen("tcp", s.opts.Address)` — 端口绑上，**还没有人 accept** | `:217` |
| 2 | `resolveAdvertise(...)` — 决定对外广播的 host:port | `:223` |
| 3 | `s.opts.Registry.Expose(...)` — **实例此刻已可被发现** | `:241` |
| 4 | `s.health.SetServingStatus(..., SERVING)`（`New` 时是 NOT_SERVING，`:108`） | `:264` |
| 5 | `close(s.ready)` — `Listen()` 的同步点 | `:268` |
| 6 | `s.grpc.Serve(lis)` — accept 循环从这里才开始 | `:270` |

第 3 步到第 6 步之间就是窗口。窗口宽度是"Expose 返回到进 accept 之间那几条语句"——这是**代码读出来的，没有量**（要量得在 `:241` 与 `:270` 之间插桩）。

## 后果：不是 connection refused，是挂到 deadline

直觉会说"对端顶多连不上，报 connection refused"。实测不是：端口已经 bind，内核照样完成三次握手并把连接塞进 accept 队列（本机 `kern.ipc.somaxconn: 128`），所以 **TCP 连接成功**；随后 grpc-go 要等服务端 preface 才认为传输可用（`google.golang.org/grpc@v1.84.0/internal/transport/http2_client.go:472`，原文 "Block until the server preface is received successfully or an error occurs"），而 preface 只有 `grpc.Serve` 跑起来才会出现。于是通道一直停在 `CONNECTING`——`WaitForReady=false` 的 fail-fast 只在 `TRANSIENT_FAILURE` 生效，救不了这一发。

装置：stock grpc-go（pin 到 sweet 用的 v1.84.0），一个绑了但不调 `Accept` 的 `net.Listener`，用 `grpc.health.v1.Health/Check` 打一发，2 秒 deadline。读数：

```
listener bound on 127.0.0.1:30777, no Accept loop running
raw TCP dial           succeeded (kernel completed the handshake into the backlog)
inside window (2s)     elapsed=2s       code=DeadlineExceeded
after Serve started    elapsed=0s       code=OK  status=SERVING
```

第三条与第四条是重点：**窗口内的调用挂到自己的 deadline**，而不是被拒；`grpc.Serve` 一起，**同一条 `ClientConn` 立刻成功，没有任何人重新拨号**。所以这个窗口的性质是**延迟**，不是错误——窗口内发出的那一发失败，之后自然恢复。

对照组（端口从未 bind，见下一节 go-zero 的位置）：

```
port never bound: elapsed=0s code=Unavailable
  err=... transport: Error while dialing: dial tcp 127.0.0.1:64352: connect: connection refused
```

## 四框架在这条轴上的位置

| 框架 | 发布相对 bind | 发布相对 accept | 窗口症状 |
|---|---|---|---|
| go-zero | **bind 之前** | 之前 | 立刻 `UNAVAILABLE`（connection refused） |
| sweet | bind 之后 | **之前** | 挂到 deadline（`DEADLINE_EXCEEDED`） |
| dubbo-go（grpc 协议） | bind 之后 | **也是之前** | 同 sweet，且更不确定 |
| jaws | bind 之后 | **之后** | 无此窗口 |

证据（行号取自本机 checkout：dubbo-go `v3.3.2-20260910`；go-zero 的 `git describe` 命中的是嵌套 tag `tools/goctl/v1.10.2-67-g2b5efac9`，主仓版本不能拿它说事，和 `~/github/nacos` 那个坑一样）：

- **go-zero**：`zrpc/internal/rpcpubserver.go:48-53` —— `keepAliveServer.Start` 先 `registerEtcd()`，返回后才 `s.Server.Start(fn)`；而 `net.Listen` 在 `zrpc/internal/rpcserver.go:48`、`server.Serve(lis)` 在 `:78`。etcd 那条 key 写下去时端口都还没绑，中间只隔着 `grpc.NewServer` 与拦截器装配。
- **dubbo-go**：`registry/protocol/protocol.go:293` `doLocalExport(...)` → `:307` `reg.Register(registeredProviderUrl)`，看着像"先服务后发布"。但 grpc 实现里 `protocol/grpc/server.go:90` 只同步做 `net.Listen`，`server.Serve(lis)` 在 `:148` 起的 goroutine 里，而且那个 goroutine 还要先 `waitGrpcExporter(...)` 才 `registerService` 才 Serve；`openServer` 在 `:161-162` 直接返回，**没有任何等待**。所以注册完全可能抢在 accept 循环起跑之前。另外 `protocol.go:296` 那行 `// health.SetServingStatusServing(...)` 是注释掉的——健康状态翻转没走这条路。
- **jaws**：`ServiceConfig.java:254` `protocol.export(provider)` → `AbstractNettyServer.java:139-143` `bind(...).syncUninterruptibly()` 且 `state = ChannelState.ALIVE` → `ServiceConfig.java:258` 才 `register(...)`。四个里唯一真正"accept 起来了才发布"的。

**没核的部分，别当结论用**：dubbo 协议的 getty 传输没验——`remoting/getty/getty_server.go:234-235` 是 `getty.NewTCPServer(...)` + `RunEventLoop(...)`，但 `dubbo-getty` 不在本机 module cache 里，bind/accept 时机没读到源码。triple/dubbo3 那条（`protocol/triple/triple_protocol/server.go:200-206` 的 doc 写着 "Start … without blocking"，意味着注册可能抢在 bind 之前）是二手，没逐行复核。

## 反直觉的一条：更"糟"的顺序，症状反而更好

go-zero 把发布放在 bind 之前，按"publish last"的原则这是最糟的位置，但它的症状是**立刻 `UNAVAILABLE`**：客户端拿到明确的失败、grpc-go 自己进 backoff 重连、调用方重试或被 LB 换到别的实例。sweet 现在的中间位置拿到的是 `DEADLINE_EXCEEDED`——连接建上了却没人读，客户端没有任何失败理由，只能干等。

所以真要动次序，目标不是"随便换个顺序都行"，而是明确奔 jaws 那种（accept 已开始才发布）。**换到 go-zero 那一侧不叫修复。**

## 为什么保持现状（2026-11-04 定案）

窗口极窄（第 3 步返回到第 6 步之间几条语句），症状是延迟而非错误，且 `Serve` 一起来就自愈。改成 accept-first 是行为改动，代价四件事，都不小：

1. `grpc.Serve` 阻塞 ⇒ 必须 `go s.grpc.Serve(lis)`，`Serve()` 自己再等它返回，返回值语义要重新定。
2. 需要一个"accept 真的开始了"的同步点——包一层 `net.Listener`，第一次 `Accept()` 被调用时关掉一个 channel。**不能用 sleep，也不能拿"goroutine 已启动"当证据。**
3. 发布失败的语义要定：注册不上是"起了又停"（此时 socket 已经 accept 过）还是"根本不起"（现在的 fail-fast）。
4. `Listen()` 的契约要跟着动：它现在注释写着 "blocks until the listener is bound **and exposed**"，`ready` 在 Expose 之后才关；挪序后 `ready` 变成"bound + accepting + exposed"，而 `Addr()`/`Port()` 只需要 bound——大概要把 `ready` 拆成 bound / published 两个信号，否则 `-listen :0` 那种"先绑上拿端口"的用法会被注册的往返拖住。

**决定：不改。** 这条不是待办，下次别当新发现重新提一遍；要翻这个决定，先把上面四件事的方案写出来。

## 关停侧不是启动侧的镜像

`Stop()`（`:275`）的次序是：`health.Shutdown()`（所有状态翻 `NOT_SERVING`）→ 注销 → 等 `DrainDelay` → `GracefulStop`。也就是说**状态翻成 NOT_SERVING 时 socket 还在应答**——这是有意的：探针要不比注册中心更晚知道"别再发了"，同时在途调用还能跑完。

因此启动侧那两条性质在关停侧都不成立：关停期间读到 `NOT_SERVING` 的地址**可能**仍然在服务。"graceful"本来就是不对称的——发布要晚于可服务，撤回要早于停止服务。
