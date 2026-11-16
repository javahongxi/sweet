# gin 与 grpc-go 的 goroutine 模型对比

本文只谈事实与取舍，不改变 Sweet 的任何代码；Sweet 也没有引入 gin，唯一第三方依赖仍是
`google.golang.org/grpc`。写这篇的动机是：HTTP 框架和 gRPC 都跑在同一个 Go runtime 上，
`GOMAXPROCS`、调度器、GC 完全一样，但**一次请求对应哪个 goroutine、这个 goroutine 活多久、
socket 由谁读写** 这三件事差别很大，直接决定了并发上限、过载行为、故障半径、性能表现和泄漏形态。

结论锚定的版本：Go 标准库 `net/http`（本机 GOROOT）、grpc-go v1.84.0。

## 1. 一句话结论

- **gin**：一个 TCP 连接一个 goroutine，handler 在这个 goroutine 里同步读 body、跑业务、写 response，
  业务 goroutine 就是 socket goroutine。
- **grpc-go**：一条连接上常驻 reader / loopy writer / keepalive 三类 goroutine，
  服务端**每个 RPC** 再起一个 handler goroutine；handler 从不直接碰 socket，
  读写都通过内部缓冲与专用 goroutine 交接。

## 2. 对照表

| 维度 | gin（net/http） | grpc-go |
| --- | --- | --- |
| goroutine 粒度 | 连接 | 连接（固定 3 类） + stream（每个 RPC 一个） |
| goroutine 生命周期 | 与 TCP 连接等长，keep-alive 期间反复服务多个请求 | handler goroutine 与单个 RPC 等长，结束即回收 |
| 并发复用 | HTTP/1.1 一条连接同时只有一个请求 | HTTP/2 一条连接多路复用 N 个 stream |
| socket 读写归属 | handler goroutine 自己读写 | reader / loopy writer goroutine，handler 只收发已解码消息 |
| 限并发手段 | 无内置，自己加信号量或 `MaxConns` | `MaxConcurrentStreams` + `newHandlerQuota`，可选 `NumStreamWorkers` 工作池 |
| worker pool | 无 | 服务端有（`NumStreamWorkers(n)`），客户端没有也不需要 |
| 取消传播 | 靠 `Request.Context()` 与 body 读写失败 | reader 观察到 RST_STREAM/GOAWAY 后 cancel stream ctx |
| handler panic 半径 | `net/http` recover 掉，**连接关闭**；`gin.Recovery()` 转 500，连接存活 | **不 recover**，panic 直接打死进程 |
| 异步返回后 | `gin.Context` 失效，需 `c.Copy()`，不能并发写 `c.Writer` | stream 由 handler goroutine 独占，`SendMsg/RecvMsg` 不可跨 goroutine 并发 |
| 背压体现 | 写 `ResponseWriter` 阻塞在连接 goroutine 上 | `SendMsg` 阻塞在 handler goroutine 的流控配额上，真正写 socket 的仍是 loopy |

## 3. gin / net/http：连接即 goroutine

`Server.Serve` 的 accept 循环每收一个连接就起一个 goroutine：

```go
// net/http/server.go:3604
go c.serve(connCtx)
```

`conn.serve` 内部是一个 for 循环：读一个请求 → 调 `handler.ServeHTTP` → 若 keep-alive 则继续读下一个。
gin 的 `Engine.ServeHTTP` 只是这条链路上的 handler，它做路由树匹配和 middleware 链拼装，
**不额外创建任何 goroutine**。因此：

- **粒度是连接，不是请求**。10k 空闲 keep-alive 连接 ≈ 10k 个常驻 goroutine，
  每个 goroutine 初始栈 8KB，再加上 `http.Conn`/`bufio` 读写缓冲。
- **一个连接同一时刻只处理一个请求**。没有 HTTP/2 多路复用，浏览器/客户端要靠开多条连接来提升并发，
  于是 goroutine 数与连接数线性相关。
- **业务代码就是 I/O 代码**。`c.Request.Body` 的读取和 `c.Writer` 的写入发生在同一个 goroutine 上，
  没有中间层，也就没有"消息已经解码好放在 buffer 里等你"这种解耦。
- **想异步只能自己 `go func()`**，而 handler 一旦返回，`ResponseWriter` 就被复用、
  `Request.Context` 被 cancel，所以必须 `c.Copy()` 后再带走，且不能从多个 goroutine 写同一个 `c.Writer`。
- **panic 半径**：`conn.serve` 的 defer 会 `recover()` 并打 `http: panic serving ...`，然后**整个连接被丢弃**；
  挂了 `gin.Recovery()` 时 panic 在 middleware 层就被吃掉，变成 500，连接继续服务后续请求。

## 4. grpc-go：连接级常驻 goroutine + 每个 RPC 一个 handler

以 v1.84.0 为参照，两侧的连接级 goroutine 是固定的：

| 位置 | goroutine | 职责 |
| --- | --- | --- |
| `internal/transport/http2_client.go:420` | `go t.reader(errCh)` | 解帧、流控、分发到各 stream 的 recvBuffer |
| `internal/transport/http2_server.go`（`HandleStreams`） | 连接 goroutine 本身 | 同样的读帧循环，遇到 HEADERS 就回调新 stream |
| `http2_client.go:476`、`http2_server.go:348` | `newLoopyWriter` + `loopy.run()` | 唯一写 socket 的 goroutine，从 `controlBuf` 取帧 |
| `http2_client.go:410`、`http2_server.go:371` | `go t.keepalive()` | ping 探活与超时判定 |

服务端每个进来的 RPC 都会起一个 goroutine 跑 handler：

```go
// server.go:1090-1108（节选）
streamQuota := newHandlerQuota(s.opts.maxConcurrentStreams)
st.HandleStreams(ctx, func(stream *transport.ServerStream) {
    s.handlersWG.Add(1)
    streamQuota.acquire()
    f := func() {
        defer streamQuota.release()
        defer s.handlersWG.Done()
        s.handleStream(st, stream)
    }
    if s.opts.numServerWorkers > 0 {
        select {
        case s.serverWorkerChannel <- f:
            return                     // 交给固定 worker 池
        default:                        // 池满则退回 go f()
        }
    }
    go f()
})
```

几个和 gin 结构性不同的点：

- **handler 不碰 socket。** 发送走 `t.write()` → 造一个 `dataFrame` → `t.controlBuf.put(df)`
  （`http2_client.go:1128-1156`），由 loopy writer 统一编码、合并 HEADERS/DATA、按 stream 权重调度后写出去。
  这就是为什么 grpc-go 能在同一个 goroutine 上做帧聚合与优先级控制。
- **接收是显式交接。** reader goroutine 把 DATA 帧变成 `recvMsg` 塞进 stream 的 `recvBuffer`
  （`transport.go:96`：`chan recvMsg` 容量 1 + mutex 保护的 `backlog` 切片），
  handler 里的 `RecvMsg` 阻塞在这个 channel 上。**reader goroutine 与 handler goroutine 之间是一次 channel 同步**，
  而 net/http 里这两件事在同一个 goroutine 上顺序发生。
- **背压仍然体现在业务 goroutine 上。** `SendMsg` 之前要过流控配额 `s.wq.get(len(hdr)+dataLen)`，
  窗口不够时是 handler goroutine 阻塞，而不是 loopy。写侧解耦不等于业务不受 I/O 节奏约束。
- **取消由 reader 观察。** 对端 RST_STREAM 被 reader goroutine 看到 → `closeStream` → cancel handler 的 ctx。
  也就是说 handler 里 `ctx.Done()` 的信号来自"另一个 goroutine"，
  而 gin/net/http 里客户端断开的感知更依赖你自己正在做的读写操作失败。
- **客户端不额外起 goroutine。** 一元调用的发起方 goroutine 直接阻塞等 `ClientStream` 的 done/recvBuffer，
  所以 grpc-go 客户端的 goroutine 数 ≈ 连接数×3 + 并发调用数，而不是每 RPC 一个"驱动"goroutine。
- **并发上限是内建的。** `MaxConcurrentStreams` 同时约束 HTTP/2 广告给对端的窗口和本地并发 handler 数
  （`newHandlerQuota`）；`NumStreamWorkers(n)` 可以把"每 RPC 一个 goroutine"换成固定 worker 池
  （`server.go:635`），代价是单个慢 handler 会占住 worker，队首阻塞由池子承担。
- **panic 半径最大。** grpc-go 服务端不 recover：v1.84.0 里非测试代码只有一处 `recover()`，位于
  `internal/xds/clients/xdsclient/channel.go`，服务端 handler 调用链上没有任何兜底，
  文档也把 panic 当作应避免而非应捕获的东西。业务 handler 里一个 nil map 写入会直接崩掉整个进程，
  需要自己写 recovery interceptor 把它转成 `codes.Internal`。

## 5. goroutine 数量估算

| 场景 | gin | grpc-go |
| --- | --- | --- |
| 空闲连接 C | ~C | 服务端 ~C×3；客户端 ~C×3 |
| 在途请求 R | min(R, C)（受连接数限制） | ~R（handler goroutine，除非配了 worker 池） |
| 上限 | 连接数 | 连接数×3 + `MaxConcurrentStreams`×连接数 |

典型后果：长连接、高 QPS 的服务里，gin 的 goroutine 数远低于 QPS，而 grpc-go 的 goroutine 数接近瞬时并发 RPC 数——
但这批 goroutine 死得快，且每个都只做业务，不做 syscall 层的字节搬运。

## 6. 性能差异表现在哪些方面

先定口径：6.1–6.7 是**机制推导**（谁在哪个 goroutine 上做什么、每次交接花什么），
只有 6.8 是本仓实测数字，而且**只覆盖 gRPC 一侧**。gin 一侧没有测过，不要把这一节读成实测结论。

### 6.1 连接成本摊销：差距最大的一项

HTTP/1.1 没有多路复用，并发靠多连接。每条新连接都要付 TCP 三次握手（TLS 再付 1–2 个 RTT）和慢启动窗口，
前几个 RTT 的吞吐被窗口卡住。gin 侧 goroutine 数与连接数线性相关，是同一件事的另一面：
**每加一点并发就多一套连接状态**。

gRPC 走 HTTP/2，一条连接跑满 N 个 stream，握手与慢启动一次性摊销，之后连接一直是热的。
v1.84.0 的初值：stream 初始窗口 `initialWindowSize = 65535`、出向字节配额 `defaultWriteQuota = 64 * 1024`
（`internal/transport/defaults.go`），实际窗口由 BDP estimator 动态调。

**表现**：内网东西向、高 QPS 小消息，gRPC 在吞吐和尾延迟上都占优，CPU 不随并发线性膨胀；
低频公网调用，两者差距被 RTT 淹没。

### 6.2 序列化与每请求固定开销

- **HTTP + JSON**：文本协议，header 是 `map[string][]string`（每次解析都要分配），body 走 `encoding/json` 反射，
  string 与 []byte 来回转换，逃逸对象偏多。
- **gRPC + protobuf**：字段号驱动的生成码，无反射；DATA 帧 buffer 走 `mem.BufferSlice` + `mem.BufferPool`，有复用；
  metadata 走 HPACK，重复头部只发索引。

**表现**：小消息高频调用时差异最明显（序列化占比高）；body 很大时差异被内存拷贝与带宽主导。
反过来，HTTP 侧若开 gzip，每请求一次压缩的成本通常大于 protobuf 省下的；gRPC 默认不压缩，压缩是 per-call 选项。

### 6.3 写侧聚合 vs 直接写

grpc-go 所有出站帧先进 `controlBuf`，由唯一的 loopy writer 取出，一次唤醒里尽量多编码几帧再写 socket，
缓冲大小由 `bdpEst` 决定。这带来帧聚合，也多带来一次 channel 交接。
net/http 里 handler 直接写 `bufio` 缓冲并 flush，零交接，但每个响应自己承担 chunked 编码与头部文本。

**表现**：高并发下 gRPC 的 syscall 更少；低并发、大响应时 gin 侧的"直接写"路径更短，单请求延迟下界更低。

### 6.4 交接与 goroutine 生命周期成本（gRPC 的代价面）

一次服务端 unary 调用在 grpc-go 里至少走：reader 解帧 → `recvBuffer.put`（`chan recvMsg` 容量 1 + backlog）
→ handler goroutine 从 `RecvMsg` 醒来 → `SendMsg` 过 `controlBuf.put` → loopy 醒来编码 → 写出 socket。
中间两次跨 goroutine 唤醒，外加一个每 RPC 新建/销毁的 handler goroutine。
gin 侧这条链在同一个 goroutine 上顺序完成：零唤醒、零新建。

**表现**：极短 RPC × 极高并发时，这部分（唤醒、栈分配、调度）占可观测比例的 CPU。
`NumStreamWorkers(n)` 可以把它换成固定 worker 池来摊平，代价是慢 handler 占住 worker 造成队首阻塞——
省了创建成本，换来排队延迟。

### 6.5 过载时的行为（比"谁更快"更该关心）

- **gin/net/http 没有内建 admission control**。上限由连接数、FD、goroutine 与内存决定；
  过载表现为连接和 goroutine 堆积、内存爬升，直到超时雪崩。限并发要自己加。
- **grpc-go 的过载会顶到读循环**。`newHandlerQuota`（`server.go:1987`，底层是 `atomicSemaphore`）的
  `acquire()` 在 `HandleStreams` 回调里**同步**调用，配额耗尽时 reader 循环停在 acquire 上，
  不再处理该连接后续帧（包括别的 stream 的 DATA 和 WINDOW_UPDATE），背压退回 TCP 层，
  客户端表现为调用排队。另一处是 `controlBuf.throttle()`（`controlbuf.go:349`），
  响应帧积压超过阈值（默认 100，可用 `GRPC_GO_EXPERIMENTAL_CONTROL_BUFFER_THROTTLE_LIMIT` 调）时同样停住读循环。
  `MaxConcurrentStreams` 同时也是对端可开 stream 数的上限，所以这个信号在客户端就已经生效。

**表现**：同样过载，gin 更像"先全收下来然后一起变慢"，grpc-go 更像"在这条连接上先停住"。
后者对进程存活友好，但**同连接上的其他 RPC 会一起被 stall**——要避开就减小 streams 数或另加限流层。

### 6.6 内存画像

| 项 | gin/net/http | grpc-go |
| --- | --- | --- |
| 空闲连接 | 1 goroutine（栈 8KB 起）+ 读写 bufio 缓冲 | 3 goroutine + framer + HPACK 表 + activeStreams map + controlBuf，单条更贵 |
| 在途请求 | body 无默认上限，需 `http.MaxBytesReader` | 默认接收消息上限 4MB；未读消息堆在 `recvBuffer.backlog`，慢 handler 会真占内存 |
| 海量空闲 | 相对便宜 | 需要 `MaxConnectionIdle` / `MaxConnectionAge` 与 keepalive EnforcePolicy 主动回收 |

**表现**：C10K+ 的空闲长连接是 gRPC 侧弱项（每连接常驻 3 个 goroutine 加协议状态）；
大 body 上传是 HTTP 侧弱项（没有内建上限）。

### 6.7 GC

grpc-go 的对象"多而小、生命周期短"，关键 buffer 还有 pool；net/http + JSON 的对象"少而大"，
header map 与 body 字节更容易逃逸并活过一代。同样十万 QPS 下，前者多体现在 GC 频次，
后者多体现在扫描成本与 assist。但真实服务里业务自身的分配通常远大于框架分配，**这一项经常被高估**。

### 6.8 本仓能引用的数字，以及不能引用的

`bench/README.md` 里 gRPC 一侧的真机实测（Apple M2 8 核、go1.27.3、grpc-go v1.84.0、loopback，每点 3 遍取中位）：

| 并发 | QPS | P50 | P99 |
| --- | --- | --- | --- |
| 20 | 116,653 | 161 µs | 384 µs |
| 80 | 154,273 | 485 µs | 1,020 µs |
| 160 | 165,870 | 927 µs | 1,677 µs |

饱和平台在 160–480 线程，560 起塌到 ~131k（-14%）；sweet 相对裸 grpc-go 的每调用开销在 ±8% 分辨率下测不出来。

这些数字**不能**用来回答"gin 和 grpc-go 谁快"：bench 只有一条 gRPC 臂，把它和某个 HTTP 框架的数字并排，
一次比较里同时换了序列化、协议、连接模型和运行时，差值只能记成"栈 vs 栈"。真要补 gin 臂，得按同一口径：
同机器、同 payload 形状、A/B/A/B 交错、每点 3 遍取中位、同并发点位报告，并且确认两端 handler 都是静默的
——bench 里那条 `log.info` 把 Java 服务端从 104k 打到 39.3k 的反例就是教训。

## 7. 泄漏与故障形态差异

- **gin 的泄漏**通常是：handler 里 `go func()` 没退出、异步 goroutine 还引用着 `c` / `c.Writer`、
  或者忘读 `Request.Body` 导致连接 goroutine 停在读循环。
- **grpc-go 的泄漏**通常是：stream handler 没 return（`RecvMsg` 等一个永不到来的消息，而调用方没设 deadline）、
  interceptor 里把 stream 交给另一个 goroutine 后又并发调用 `RecvMsg`。
  同一条 stream 的 `RecvMsg` 只能有一个 goroutine 调用，两个 goroutine 双读是竞态而不是并发加速。
- **崩溃半径**：gin 的一个 panic 影响一个连接（有 Recovery 时只影响一个请求）；
  grpc-go 的一个 panic 影响整个进程。这也是为什么框架层（Sweet 这类）需要自己补 guard 和 recovery。

## 8. 与 Sweet 的关系

Sweet 的网关正好横跨这两种模型：`gateway.Gateway` 实现 `http.ServeHTTP`，跑在 `net/http` 的连接 goroutine 上；
转码后发起的 gRPC 调用则进入 grpc-go 的连接级 reader/loopy 与 stream 级 handler。
一次 HTTP-to-gRPC 请求在两侧各有自己的 goroutine 归属，排查"慢在哪"时要先分清是哪一段。

另外，`nacos/conn.go` 的 `readLoop` / `keepAlive` 与 `lifecycle.go` 的 `supervise` 这一组长生命周期
goroutine，和 grpc-go transport 的 reader / loopy writer / keepalive 是同构的职责划分：
一个 goroutine 只做读、一个只做写、一个只管定时器。而 grpc-go 不给服务端 handler 兜 panic，
Sweet 在自己的常驻 goroutine 上显式加了 guard（`defer c.guard("readLoop")`），
把 panic 转成"流死亡事件"而不是进程退出——这是对同一类问题的相反取舍：
连接级 goroutine 崩了必须能重连，业务 handler 崩了应当暴露出来。

## 9. 自查命令

```bash
# 本机 grpc-go 版本与关键行
go list -m google.golang.org/grpc
grep -n "go f()" $(go env GOMODCACHE)/google.golang.org/grpc@v1.84.0/server.go
grep -n "newLoopyWriter\|go t.keepalive" $(go env GOMODCACHE)/google.golang.org/grpc@v1.84.0/internal/transport/http2_server.go

# 标准库：连接级 goroutine
grep -n "go c.serve" $(go env GOROOT)/src/net/http/server.go

# 流控初值与配额阈值
grep -n "initialWindowSize\|defaultWriteQuota" $(go env GOMODCACHE)/google.golang.org/grpc@v1.84.0/internal/transport/defaults.go

# 本仓 gRPC 臂的基准跑法（gin 臂需另写）
go run ./bench -mode provider -server native -port 50051
go run ./bench -mode consumer -client native -addr 127.0.0.1:50051 -threads 160 -warmup 5 -duration 40

# 运行时观察：两类 goroutine 的数量差
curl -s localhost:6060/debug/pprof/goroutine?debug=1 | head -40
```
