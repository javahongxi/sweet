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
`-print-status-table`（打印 gRPC→HTTP 映射表后退出）、`-routes`（`embedded` | `nacos`）、
`-routes.dataId`、`-routes.group`、`-route-cache`（把最后一次被接受的路由表留在盘上，
冷启动时兜底；默认空 = 不碰磁盘）、`-namespace`（Nacos 命名空间，配置域里那个字段叫 tenant）。

`direct` 那一档是给你只想验线格式、不掺进发现时用的：

```
go run ./example/gateway -registry=direct -endpoints 127.0.0.1:20001
```

## 路由表来自配置中心（`-routes=nacos`）

默认 `embedded`：两条路由写在 Go 里，开箱可跑。加上 `-routes=nacos` 之后，路由表变成一个
Nacos 配置项——**映射是数据，消息类型仍然是编译期的 Go**（`gateway.Options.Services` 那张表）。
格式与契约的来由见 `doc/nacos-config-routes.md`。

发布表（Nacos 3.x 的管理口是 v3，参数名是 `groupName`，不是 v1 那套 `group`）：

```
curl -s -X POST 'http://127.0.0.1:8848/nacos/v3/admin/cs/config' \
  --data-urlencode dataId=sweet-gateway-routes.json \
  --data-urlencode groupName=DEFAULT_GROUP \
  --data-urlencode 'content={"routes":[{"method":"POST","path":"/v1/greeter/greet",
    "service":"sweet.example.v1.Greeter","fullMethod":"/sweet.example.v1.Greeter/Greet"}]}'

go run ./example/gateway -routes=nacos -listen 127.0.0.1:20112
```

实测（2026-11-02，本地 Nacos 3.2.3，同一次运行、同一端口 20112，全程没重启网关）。
先只发一条一元路由：

```
2026-11-02 20:46:41 config: connected to 127.0.0.1:9848 as 1793623601388_127.0.0.1_58453 (its own SDK connection; the nacos registry keeps a separate one)
2026-11-02 20:46:41 routes: applied 1 route(s) from DEFAULT_GROUP/sweet-gateway-routes.json (md5 fe8fdd267ca343d4b34eeced92392192)
2026-11-02 20:46:41   POST /v1/greeter/greet -> /sweet.example.v1.Greeter/Greet via sweet.example.v1.Greeter
```

```
$ curl -s localhost:20112/v1/greeter/greet -d '{"name":"web"}'
{"message":"Hello, web!"}
```

那个 md5 与 `curl` 读回配置时服务端报的 md5 逐字相同，说明客户端解出的是服务端的值而不是自己算的。

再发第二条（加服务端流路由），两秒后日志与响应：

```
2026-11-02 20:46:56 routes: applied 2 route(s) from DEFAULT_GROUP/sweet-gateway-routes.json (md5 de769ee53183bb45ee641a98b91a1645)
2026-11-02 20:46:56   POST /v1/greeter/greet -> /sweet.example.v1.Greeter/Greet via sweet.example.v1.Greeter
2026-11-02 20:46:56   POST /v1/greeter/greet-stream -> /sweet.example.v1.Greeter/GreetStream via sweet.example.v1.Greeter

$ curl -sN localhost:20112/v1/greeter/greet-stream -d '{"name":"web","times":3}'
{"message":"Hello #1, web!"}
{"message":"Hello #2, web!"}
{"message":"Hello #3, web!"}
```

第三发是这一节真正要验的那件事——**坏表不能把线上表带走**。故意发一条指向未注册方法的路由：

```
2026-11-02 20:46:58 routes: apply failed: gateway: route POST /v1/greeter/greet: method "Nope" of service "sweet.example.v1.Greeter" is not registered (known: Greet, GreetStream) — live table untouched
```

```
$ curl -s -o /dev/null -w '%{http_code}\n' localhost:20112/v1/greeter/greet -d '{"name":"after-bad"}'
200
$ curl -s -o /dev/null -w '%{http_code}\n' localhost:20112/v1/greeter/greet-stream -d '{"name":"x","times":1}'
200
```

错误消息里带着 `known: Greet, GreetStream`，因为运维在改一张四十条的表时需要的不是"错了"，
而是"这个名字该写成什么"。两条旧路由在坏表之后仍然 200，就是 `ApplyRoutes` 全有或全无的兑现。

三个刻意的形状：

- **配置域自己一条连接**。Nacos 按 `module` 标签认连接，命名模块对非 `naming` 的连接直接忽略
  （`ConnectionBasedClientManager.java:60-64`），所以 config 不能搭注册中心那条车的便车；
  日志第一行把这件事写在脸上。域内倒是复用：query 与 listen 走同一条 SDK 连接。
- **只注册网关真能服务的两个方法**。客户端流与双向流没进这张表，所以配置里写 `GreetAll` 会在
  apply 阶段被拒，而不是被当成一元调用半路走歪。
- **空表被拒**。删掉配置和"不路由任何请求"是两种意图，让边缘因为一个 dataId 被删而全 404 是更贵
  的那种错。要真清空，Go 侧改默认值，别指望配置。

### 配置中心不可达时冷启动（`-route-cache`）

`-routes=nacos` 让网关的启动多了一个依赖：配置中心。默认行为是"拿不到表就一条都不路由"，
所以 `-route-cache <文件>` 把最后一次**被接受的**表留在盘上，冷启动先装它，live 表到了再覆盖：

```
go run ./example/gateway -routes=nacos -route-cache "$HOME/.cache/sweet/gateway-routes.json"
```

默认空 = 完全不碰磁盘，与注册中心的快照目录是同一套开关纪律（一份没人问过年龄的表是负担，
不是福利）。给绝对路径，别让缓存落在当前工作目录里。

实测（2026-11-02 21:35）：网关 #1 连真 Nacos 写出 295 字节缓存后停掉；网关 #2 把 `-server`
指向无人监听的 19999、后端改用 `-registry=direct` 保住，于是这一发隔离出的正是
「配置中心不可达、后端活着」：

```
2026-11-02 23:53:00 route cache: restored 1 route(s) (age 2h17m4s) from …/routes.cache.json — stale until the config centre answers
2026-11-02 23:53:03 config: nacos: server unavailable: none of [127.0.0.1:19999] ([127.0.0.1:19999] on cooldown) reachable (last error: nacos: server check 127.0.0.1:19999: … connection refused) — retrying in the background
2026-11-02 23:53:03 watch DEFAULT_GROUP/sweet-gateway-routes.json: nacos: server unavailable: no connection to [127.0.0.1:19999] … — registered, will pull once the config centre answers

$ curl -s localhost:20120/v1/greeter/greet -d '{"name":"cold"}'
{"message":"Hello, cold!"}
```

三条日志各有分工：第一条说"我在服务一份旧表、旧到什么程度"，第二条说"我还在后台重试"，
第三条说"订阅已经登记上了，配置中心一活就自动补拉，不用重启"。缓存只在换表成功**之后**才写，
所以一张被拒的表永远进不了盘——这条由 `TestRouteCacheNotWrittenOnFailedApply` 钉住；其余
`TestRouteCache*` / `TestRestoreRoutes*` 五条分别钉住回读可用、来源不符拒装、无时间戳与空表拒装、
表已解析不出时拒装、以及默认关闭时不产生任何磁盘 I/O。

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
| 配置里的表能解析成路由、类型按服务名绑定 | `TestApplyRoutesResolvesTypesAndServes` |
| 一条坏路由让整张表被拒，线上表一字不动 | `TestApplyRoutesRejectsBadTableAndKeepsTheLiveOne` |
| 真实服务器上换表立即生效：旧路径 404、新路径 200 | `TestApplyRoutesSwapsLiveOnRealServer` |
| 表里 `"post"` 也能匹配真实请求；重复条目被拒 | `TestApplyRoutesNormalizesMethodCase`、`TestApplyRoutesRejectsDuplicateWithinOneTable` |
| 配置里拼错的键不会被静默丢掉，空表不会被装上 | `TestParseRouteSpecsRejectsUnknownKey`、`TestParseRouteSpecsRefusesEmptyTable` |
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
