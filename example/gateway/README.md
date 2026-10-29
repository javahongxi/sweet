# HTTP 边缘：一个网关把 curl 变成一次 gRPC 调用

对端是 `example/greeter` 那个 grpc-go 服务端，**一行没改**，注册方式也没改：它仍然用手写的
Nacos 命名协议把自己登记成 `sweet.example.v1.Greeter`。这一发证明的是网关的形状——gateway 是
`sweet` 那套 `registry.Registry` 抽象的**又一个消费者**，和服务端、客户端用的是同一个接口，
所以它不需要 provider 为它准备任何东西。

它不是 `sweet` 的传输层。服务之间仍然只走 gRPC，HTTP 只存在于外面那一跳。

## 跑

前提：本地 Nacos 3.x 在 `127.0.0.1:8848`（gRPC 口自动取 `9848`），鉴权关闭。三个终端：

```
# 终端 1：provider
go run ./example/greeter/cmd/server -registry=nacos -server 127.0.0.1:9848 -listen 127.0.0.1:20001

# 终端 2：gateway
go run ./example/gateway -registry=nacos -listen 127.0.0.1:20010 -timeout 1s

# 终端 3：打
curl -s localhost:20010/v1/greeter/greet -d '{"name":"web"}'
curl -sN localhost:20010/v1/greeter/greet-stream -d '{"name":"web","times":3}'
```

`-N` 是 `--no-buffer`：不加它 curl 也会把整条流读完才显示，看上去就像一次性返回。

入参：`-listen`、`-registry`（`nacos` | `direct`）、`-server`（Nacos gRPC 口）、
`-endpoints`（只在 `direct` 时用）、`-service`（注册中心里的查找名）、`-timeout`（调用方没给
期限时兜底）、`-verbose-errors`（把 provider 的状态文本带到边缘）、
`-print-status-table`（打印 gRPC→HTTP 映射表后退出）。

`direct` 那一档是给你只想验线格式、不掺进发现时用的：

```
go run ./example/gateway -registry=direct -endpoints 127.0.0.1:20001
```

## 实测：一元（2026-10-30 那一跑，`-timeout 1s`）

```
== 1 happy ==
HTTP/1.1 200 OK
Content-Type: application/json
Date: Thu, 29 Oct 2026 19:12:00 GMT
Content-Length: 25

{"message":"Hello, web!"}
== 2 provider business error ==
HTTP/1.1 400 Bad Request
Content-Type: application/json
Date: Thu, 29 Oct 2026 19:12:00 GMT
Content-Length: 88

{"httpStatus":400,"code":"INVALID_ARGUMENT","codeNumber":3,"message":"name is required"}
== 3 empty body ==
HTTP/1.1 400 Bad Request
Content-Type: application/json
Date: Thu, 29 Oct 2026 19:12:00 GMT
Content-Length: 88

{"httpStatus":400,"code":"INVALID_ARGUMENT","codeNumber":3,"message":"name is required"}
== 4 unknown JSON field ==
HTTP/1.1 400 Bad Request
Content-Type: application/json
Date: Thu, 29 Oct 2026 19:12:00 GMT
Content-Length: 126

{"httpStatus":400,"code":"INVALID_ARGUMENT","codeNumber":3,"message":"decode JSON: proto: (line 1:2): unknown field \"nam\""}
== 5 wrong method ==
HTTP/1.1 405 Method Not Allowed
Allow: POST
Content-Type: application/json
Date: Thu, 29 Oct 2026 19:12:00 GMT
Content-Length: 124

{"httpStatus":405,"code":"METHOD_NOT_ALLOWED","codeNumber":-1,"message":"GET is not allowed on /v1/greeter/greet; try POST"}
== 6 unknown path ==
HTTP/1.1 404 Not Found
Content-Type: application/json
Date: Thu, 29 Oct 2026 19:12:00 GMT
Content-Length: 92

{"httpStatus":404,"code":"NOT_FOUND","codeNumber":-1,"message":"no route for POST /v1/nope"}
```

然后把 provider `SIGTERM` 掉，再打一次（**没有重启网关**）：

```
== curl after provider down ==
HTTP/1.1 503 Service Unavailable
Content-Type: application/json
Date: Thu, 29 Oct 2026 19:12:08 GMT
Content-Length: 126

{"httpStatus":503,"code":"UNAVAILABLE","codeNumber":14,"message":"the gateway could not complete the request; check its logs"}
```

两边日志，逐字：

```
# provider
2026-10-30 03:11:34 sweet server up: listen=127.0.0.1:20001 advertised=127.0.0.1:20001 service=sweet.example.v1.Greeter registry=nacos
2026-10-30 03:12:05 signal received, draining (grace period 200ms)
2026-10-30 03:12:05 stopped: deregistered then drained
```

```
# gateway
2026-10-30 03:11:54 route: POST /v1/greeter/greet -> /sweet.example.v1.Greeter/Greet via nacos registry
2026-10-30 03:12:08 signal terminated, draining
2026-10-30 03:12:08 stopped: 1 route(s) served through the nacos registry
```

（`curl` 的 `Date` 头是 UTC，日志前缀是本地时区，差八小时——同一场运行，不是两次。）

## 这七发各自在说什么

**一**是这条链的形状：`{"name":"web"}` 变成一次 `/sweet.example.v1.Greeter/Greet` 调用，
回复再变回 JSON。地址来自注册中心，命令行里没有任何 host:port。

**二**和**四**都是 400，但来源不同，而响应体把这件事写清楚了：二是 provider 自己的业务校验
（`checkName` 那条 `status.Error(codes.InvalidArgument, "name is required")`）跨语言落到边缘；
四是网关在解码阶段就拒了，`message` 里带着 `decode JSON:` 前缀。**网关没有替 provider 做校验**——
这一发也说明了为什么 `-service` 与 `FullMethod` 是两个字符串：前者问注册中心，后者上线。

**三**看着像反直觉：空 body 不是 400，而是"所有字段都用默认值"的合法请求，于是它真的发出去了，
然后被 provider 用同一条 `checkName` 打回来。所以看到的是二而不是一个网关自己发明的错误。

**五**和**六**的 `codeNumber` 是 `-1`：这两个失败根本没上到线上，没有 gRPC 状态可报。用一个不可能的
数字把它们与"provider 真的回了 NOT_FOUND"分开，比让调用方去读 `code` 字符串靠谱。

**最后一发**是网关最该有的表现：provider 没了，它不猜、不重试成 200，`UNAVAILABLE` → 503。
注意这一发的 `message` 变成了 `the gateway could not complete the request; check its logs`——
这就是 `code`/`codeNumber` 与 `message` 分开的理由：**4xx 的文本描述的是调用方自己的请求，永远回显；
5xx 的文本描述的是我们的机器，默认打码**（要原文就加 `-verbose-errors`）。

## 实测：服务端流（2026-10-30 另跑一次，`-timeout 2s`）

两条路由在同一次启动里注册，网关日志先把这件事写明白：

```
2026-10-30 03:20:41 route: POST /v1/greeter/greet -> /sweet.example.v1.Greeter/Greet (unary) via nacos registry
2026-10-30 03:20:41 route: POST /v1/greeter/greet-stream -> /sweet.example.v1.Greeter/GreetStream (server stream) via nacos registry
2026-10-30 03:21:09 signal terminated, draining
2026-10-30 03:21:09 stopped: 2 route(s) served through the nacos registry
```

三发流式加一元回归，逐字：

```
== A stream 3 frames ==
HTTP/1.1 200 OK
Content-Type: application/x-ndjson
Date: Thu, 29 Oct 2026 19:20:57 GMT
Transfer-Encoding: chunked

{"message":"Hello #1, web!"}
{"message":"Hello #2, web!"}
{"message":"Hello #3, web!"}

== B stream zero frames ==
HTTP/1.1 200 OK
Content-Type: application/x-ndjson
Date: Thu, 29 Oct 2026 19:20:57 GMT
Content-Length: 0


== C stream provider error before first frame ==
HTTP/1.1 400 Bad Request
Content-Type: application/json
Date: Thu, 29 Oct 2026 19:20:57 GMT
Content-Length: 88

{"httpStatus":400,"code":"INVALID_ARGUMENT","codeNumber":3,"message":"name is required"}
== D unary regression ==
{"message":"Hello, web!"}
```

行数以 200 帧那一发核过：`curl -sN … -d '{"name":"web","times":200}'` 落地 `lines=200`，首行
`{"message":"Hello #1, web!"}`，末行 `{"message":"Hello #200, web!"}`。

**A** 是这一节的主题：一帧一行、`application/x-ndjson`、`Transfer-Encoding: chunked`。

**B** 看着像反直觉，恰恰是最关键的一条：provider 一帧都不发也是**成功**。如果网关把
`StreamDesc.ServerStreams` 留成 false，grpc-go 会把这种"零消息 + 状态 OK"改写成
`Internal: cardinality violation: received no response message from non-server-streaming RPC`
（`stream.go:1290-1298`），于是网关凭空造出一个 500。顺带注意 B 的头是 `Content-Length: 0` 而不是
`chunked`——Go 在已知空 body 时不必分块，两种都合法，客户端不该假定一定看得到 `Transfer-Encoding`。

**C** 与 **A/B** 的分别在于**时机**：错误发生在第一帧之前，HTTP 状态还没交出去，所以它按普通失败报
400 加普通错误体；一旦吐过一帧，200 就改不了了，终错只能作为最后一行带内送出，形如
`{"error":{"code":"…","codeNumber":…,"message":"…"}}`——里面**没有** `httpStatus`，因为线上状态确实是 200。
带内这条在真实 provider 上打不出来：`example/greeter` 的流方法不会中途失败，所以它的证据在测试里
（`TestServerStreamErrorMidStreamTravelsInBand`、`TestServerStreamMidStreamDeadlineIsInBand`）。

**实时性不在这几发里。** 这个 provider 收到请求就一口气答完，`-w` 量到的是
`bytes=6092 ttfb=0.000679s total=0.001726s`（200 帧），首帧与末帧几乎同时，看不出 flush。逐帧 flush
的断言只能在"有节奏的 provider"上做，测试里用 `slow:25ms:4` 造这个节奏：
`TestServerStreamFlushesAsFramesArrive` 断言第一行到达的时刻早于总时长的一半。写清这件事是因为
"实测输出全绿"和"实测证明了实时性"是两回事，前者别冒充后者。

## 一个真被跑出来的坑

第一次跑完这一串，网关日志里有两行：

```
http: superfluous response.WriteHeader call from github.com/javahongxi/sweet/gateway.writeBody (status.go:183)
```

404 和 405 那两个分支里，我先 `WriteHeader` 又调了统一写错误体的函数，等于写了两次状态行。
`httptest.ResponseRecorder` 对此一声不吭（它只保留第一次），真实 `http.Server` 会记日志——
所以修完之后补了一条挂在真实服务器上的回归测试 `TestRealServerErrorLogStaysClean`，断言那台
服务器的 `ErrorLog` 在四种失败路径加一条成功路径之后仍然是空的。这类 bug 只有端到端能撞出来。
带内错误行那条新写的路径同样只写 body 不碰状态行，流式那一跑的日志是干净的（上面网关日志逐字
贴出），`TestServerStreamFlushesAsFramesArrive` 也一并断言那台真实服务器的 `ErrorLog` 为空。

## 刻意不在这里的东西

- **客户端流与双向流**。服务端流已经做了（NDJSON、逐帧 flush、带内终错）；反过来的两种形态需要一个双工
  传输——WebSocket，或 SSE 配第二个 POST——半成品入口比没有入口更坏。
- **带内错误行的键名冲突**。流一旦吐过帧，终错就只能写成 `{"error":{…}}` 这一行。所以 IDL 里如果有
  哪个回复消息带一个顶层字段恰好叫 `error`，它和错误行就分不开了。宁可接受这个约束也没给每帧套外壳：
  为一类少见的失败把成功路径的形状改掉，是更贵的取舍。
- **传输安全**。`DialOptions` 不默认填凭据：一个面向外部的 HTTP 入口若悄悄用明文连后端，是没人
  会去看的那种 bug，所以凭据必须由调用方写明（这个例子写的是 insecure）。
- **鉴权**。网关自己不校验 token，只按白名单把 header 转成 metadata；`authorization` 默认**不**转发，
  要转就在 `Options.ForwardHeaders` 里点名。理由见 `gateway/metadata.go`。
- **请求体上限**固定 1 MiB（`gateway/transcode.go`），超限报 `INVALID_ARGUMENT`，因为 gRPC 没有
  对应 HTTP 413 的状态码，宁可让 message 里写清上限也不发明一个新码。

## 测试与边界

网关的行为全部由 `gateway/` 包自己的测试钉住，`go test -race ./gateway/` 全绿：

| 想验的事 | 测试 |
| --- | --- |
| 一次 HTTP 调用真变成一次 gRPC 调用 | `TestUnaryRoundTrip`、`TestMountableAsHandler` |
| gRPC 状态码逐个对上 HTTP 码 | `TestBackendStatusMapping`（15 个码）、`TestCanceledMapsTo499` |
| 4xx 回显、5xx 打码，且开关确实改变行为 | `TestVerboseErrorsControlsMessageEcho` |
| 未知字段默认响亮地失败，开关生效后放行 | `TestUnknownJSONField` |
| 空 body = 全默认请求 | `TestEmptyBodyMeansDefaults` |
| 路由表在注册时就拒绝畸形条目、重复条目 | `TestRouteValidation` |
| 404 / 405 的形状与 `Allow` 头 | `TestRoutingErrors` |
| 调用方的 deadline 优先于网关的兜底 | `TestDeadlinePropagation` |
| 没有可用实例 → 503 | `TestNoInstancesIs503` |
| header→metadata 白名单、`-bin` 转码、敏感头不外泄 | `TestForwardedHeaderPolicy`、`TestForwardedHeadersReachTheProvider` |
| 一个服务一条连接，不是每条路由一条 | `TestConnectionsAreSharedPerService` |
| 响应字段名保持 proto 拼写 | `TestReplyShapeIsStable` |
| 真实服务器上不再出现重复 WriteHeader | `TestRealServerErrorLogStaysClean` |
| 服务端流一帧一行，顺序正确，成功流里不混错误行 | `TestServerStreamEmitsOneLinePerFrame` |
| 零帧是成功而不是 cardinality violation | `TestServerStreamZeroFramesIsSuccess` |
| 第一帧之前的错误仍按 HTTP 状态报 | `TestServerStreamErrorBeforeFirstFrameIsAnHTTPStatus` |
| 吐过帧之后终错走带内最后一行，且不带 `httpStatus` | `TestServerStreamErrorMidStreamTravelsInBand` |
| 调用方 deadline 落在流中间 | `TestServerStreamMidStreamDeadlineIsInBand` |
| 帧是边到边 flush 的（不是攒完再发） | `TestServerStreamFlushesAsFramesArrive` |
