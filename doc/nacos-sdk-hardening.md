# nacos 包 SDK 化 · 第一步（加固）

目标：让 `nacos/` 从"sweet 内部能用"变成"外人读了也敢用"。**不改定位**——仍是
sweet 的内部依赖，不拆 module、不发版。范围收敛与验收判据先落此文件再动代码。

## 前提纠正（两条，都影响做法）

1. **鉴权必须每请求携带，不能只在建连时给。** 服务端对 `ServerCheckRequest` 是特判直接
   return（`GrpcRequestAcceptor.java:106-115`），根本不进 handler、不进 filter 链；校验点在
   每次派发的 filter 上（`RequestHandler.handleRequest:45-56` + `RemoteRequestAuthFilter:110`
   读 `request.getHeaders()`）。且 token 有 TTL（默认 18000s），连接活得比它久，
   403 的恢复路径（`ClientWorker.java:1397-1398` reLogin）也要求每次都能贴新值。
   官方每个域都这么写：`ClientWorker.java:1377`、`NamingGrpcClientProxy.java:512`、
   `LockGrpcClient.java:119`。
   → 所以 `accessGetter` 挂在 **Conn**（transport 层），两个域一次拿到，不必等 naming 生命周期下沉。
   "获取"只一次（缓存 + 到期/403 刷新），"携带"每请求。
2. **failover 是连接级，不是请求级。** 官方重试环跑完仍失败就把异常抛给调用方
   （`RpcClient.java:721-723`），只有状态从 RUNNING 翻到 UNHEALTHY 才
   `switchServerAsyncOnRequestFail()`（:717-718）——换节点影响下一次连接。

## 本步做

| # | 事项 | 落点 |
| --- | --- | --- |
| A | `Metadata.headers` 线级支持 + `accessGetter`（缓存、到期、403 作废重取、每请求注入） | `envelope.go`、`conn.go` |
| B | 响应基类统一为 `{resultCode,errorCode,message,requestId,success}`；命名域 `errorMsg`/`result` 是**死字段**（服务端不发，`Response.java` 只有那四个 getter + isSuccess），改成 `message` | `naming.go`、`config_types.go` |
| C | 稳定错误面：`ErrNotFound` / `ErrDenied` / `ErrUnavailable` / `ErrRefused`，全部 `%w` 包装，`errors.Is` 契约写进注释 | 新 `errors.go` |
| D | 日志注入：`Options.Logger *slog.Logger`，nil = 不记 | `conn.go`、`config.go` |
| E | 推送缓冲满不再静默丢：`OnPushDropped func(Push)` + 计数（配置的 notify 被丢 = 表一直旧到下次变更，对配置域不成立"可恢复"） | `conn.go` |
| F | 重连改指数退避 + 抖动（固定间隔在集群同时重启时会打配置中心） | `config.go` |
| G | config 域 serverlist：多地址、失败标记、异步换节点重连；机制写成两域共用，naming 第二步再接 | 新 `serverlist.go`、`config.go` |
| H | config 域构造形状：`New` 只在配置非法时失败；`WaitReady(ctx)` 表达"连上了"；去掉"返回可用客户端 + 非 nil error"这个陷阱 | `config.go` |

## 本步不做

1. 不拆独立 module、不发版、不写 CHANGELOG/semver（那是第二步之后的承诺）。
2. **不收敛导出面**：`SetStreamLostHook`/`CloseStream`/`ProbeFatal`/`Epoch`/`Unary`/`Call`
   仍公开。它们被 `registry/nacos` 的重连环直接用着——收敛就等于把 naming 的生命周期一起搬下来，
   那是第二步的事，混进来会让两批 drill 测试同时暴露。
3. 不做请求级 failover（在飞 unary 改投另一台）。理由见上，语义代价远大于收益。
4. 不自动 `+1000` 端口。全仓约定是把 9848 写在明面上（`-server`、README、测试），
   库内藏偏移会让抓包排查对不上号。
5. 不引 `nacos-sdk-go`、不引任何新依赖（`slog` 是标准库；鉴权取 token 走 `accessGetter`
   接口，HTTP 登录由调用方实现或本包内用 `net/http` 写默认实现——只用标准库）。
6. 不做 fuzzy watch、灰度(beta)、`encryptedDataKey` 解密、`ConfigPublish`/`ConfigRemove` 写路径。
7. naming 域的 serverlist 不在本步（机制先建好）。

## 验收判据

1. `gofmt -l` 无输出；`go build ./... && go vet ./... && go test -race -count=1 ./...` 全绿。
2. **`registry/nacos` 的既有测试一行不改就通过**——这是"没碰 naming 生命周期"的证据。
3. `TestRequestCarriesAuthHeaders`：桩服务端断言每个请求的 `Metadata.headers` 都带
   `accessToken`，且 `ConfigQueryRequest` 的 body JSON 里**没有** headers 键
   （`GrpcUtils.java:108-113` 的清空纪律）。
4. `TestTokenRefreshedOn403`：桩第一次回 `errorCode=403`，断言 accessGetter 被再次调用、
   第二次带新 token 重发、调用方最终拿到内容。
5. `TestTokenCachedNotRefetched`：10 次请求只登录 1 次（getter 计数 == 1）。
6. `TestNamingRefusalKeepsServerMessage`：桩回 `{success:false,message:"no right"}`，
   断言错误串里出现 `no right`（修 B 之前这里是空的）。
7. `TestPushDroppedIsReported`：灌爆缓冲，断言回调收到被丢的 Push 且计数对得上。
8. `TestConfigServerListRotatesOnFailure`：两台桩，第一台握手即失败，断言客户端换到第二台
   并进入 ready；`TestConfigServerListSkipsUnhealthy`：运行中 kill 第一台的流，重连落在第二台。
9. `TestWaitReadyBlocksUntilHandshake`：`New` 不报错（服务端还没起），`WaitReady` 超时返回
   `ErrUnavailable`；服务端起来后 `WaitReady` 立即返回 nil，且**没人重新 New/Watch**。
10. 真机：本地 Nacos 开鉴权重跑一次网关路由热更新（发布→2s 生效），日志块取自同一次运行。

判据 10 是唯一需要改本地 Nacos 配置的，做完把鉴权关回去。

## 本轮状态（2026-11-02）

判据 1–9 全绿（`go test -race -count=1 ./...` 六包通过，重复跑两次无新竞态；
`registry/nacos` 既有测试一行未改）。已落：

- A 鉴权（`auth.go` + `Conn` 每请求注入 + 403 作废重取一次）
- C 错误面（`errors.go`，两域共用 `Response` 基类；顺带证实命名域一直在解
  `errorMsg` 这个服务端不发的键，所以每条拒绝的原因都是空串）
- D 日志注入、E 丢弃可见化、F 指数退避 + 抖动
- G serverlist（`serverlist.go`，连接级；判据 8 真机验过：`Servers=[死地址, 真 9848]`
  → `WaitReady ok; CurrentAddr=127.0.0.1:9848` 并成功 Query）
- H 构造形状：`New` 只对非法配置失败，`WaitReady(ctx)` 表达连接状态，
  `CurrentAddr` / `ConnectionID` / `LastErr` / `PushDropped` 可读

判据 10（真机开鉴权重跑热更新）**未做**：那要重启他本地这套在跑的 Nacos
（`/Users/hongxi/ai-infra/nacos/standalone/nacos-3.2.3`），代价大于收益，留给你
自己方便时做：改 `application.properties` 的 `nacos.core.auth.enabled=true` +
`nacos.core.auth.server.identity.key/value` + `nacos.core.auth.plugin.nacos.token.secret.key`
（三者都不能留空，且鉴权开着而客户端不配 `Access` 时应当看到 `ErrDenied`）。

### 顺手修掉的一个既有竞态

`-race` 在 supervisor 开始轮询 `Ready()` 之后立刻抓到：`Conn.closed` 由 `Ready()`
在 `sendMu` 下读、由 `Close()` 在另一把 `closedMu` 下写——同一字段两把锁等于没锁。
以前没人轮询所以潜伏着。改成 `atomic.Bool`（`conn.go`）。

### 两条判据的测试写法教训

- 判据 8 第一次不绿，原因不在被测代码而在**桩不忠实**：`ConfigBatchListenRequest`
  一律回空 `changedConfigs`，真服务端要按 `isUptodate(groupKey, md5, …)` 比对
  （`ConfigChangeBatchListenRequestHandler.java:82-88`）。测不出来不代表没坏。
- 另一个是**自己写的测试死锁**：`Watch` 会同步回调一次，回调里放永久阻塞就等于
  把测试自己卡在 `Watch` 里。改成在 goroutine 里 Watch。

## 一条环境事实，影响测试写法

往文件里写凭据字面量时，`Password: "secret"` 会被替换成代理值 `***`，于是"断言里比
较口令字符串"的测试必然失败（本轮第一版就死在这）。结论不是想办法把口令写进去，而是
**测试应该断言请求形状**——两个 form 字段都送到了、token 被缓存了、403 后重新取了一次。
这本来也是更该测的东西。

## 第二步（进行中）：naming 的生命周期下沉

顺序是先钉行为再搬代码。**三个既有测试文件各归各位，不硬搬**：

- `nacos/detect_test.go` 留在 Conn 层。它保的是传输层自己的契约——服务端只结束那一条
  bidi 流时 gRPC 不给任何信号，唯一可靠探测器是没人应答的探针；应答了的探针不能拆连接；
  真 3.2.3 上一元应答、流上不应答。它用的是 `nextRequestID`/`send` 这类非导出方法，
  搬到上层就失去落点。
- `registry/nacos/probe_push_test.go` 是探针不是测试：通篇 `t.Logf`，唯一的 `t.Fatal`
  是"push channel closed"，服务端推不推、推几次它都不会红。要动它先给它断言。
- `restart_drill_test.go` 是最外环（真进程 + 真 Nacos + 真 gRPC 调用 + 按 `code+reason`
  分类失败），保留；端到端收敛性不能被单元测试替代。

新增 `nacos/naming_client.go`（连接所有权 + serverlist + 意图 + 重放 + generation 守卫）
与六条测试：

| 保证 | 测试 |
| --- | --- |
| 重连后重放注册与订阅（临时实例的活性就是那条连接） | `TestNamingClientReplaysAfterReconnect` |
| 注销优先：意图先置 not-live 再发注销，重连不得复活已撤回的实例 | `TestDeregisterSurvivesAReconnectRace` |
| 旧连接的消费者不得再 ack 或投递 | `TestStaleConnectionConsumerIsInert` |
| naming 吃 serverlist，连接级换节点 | `TestNamingClientServerListFailover` |
| 连接标签必须 `module=naming` | `TestNamingClientLabelsConnectionNaming` |
| 构造只对非法配置失败，不可达交给 `WaitReady` | `TestNewNamingClientRejectsBadOptions` |

`registry/nacos` 的既有测试一行未改即通过：这一轮只加东西，没动它。

### 这一轮自己踩的两个坑

1. 批量改文件的 python 脚本里写了 `s = ... if cond else None`，`s` 变 `None` 时文件已被
   `open(p,'w')` 截断，`naming_client_test.go` 直接清零，只能重写。教训：改文件的脚本
   不要让主变量有条件为空，且动手前先留副本。
2. `TestStaleConnectionConsumerIsInert` 第一版少一条 ack。不是客户端漏 ack——`SendMsg`
   只入队，测试在帧还没出去时就 `Kill()` 拆流。改成"先确认第一条 ack 落地再拆流"，
   顺带补上了原先根本没验的"第一条 ack 存在"。

### registry/nacos 已削成适配器（同日）

删掉它自己的 `ensureConn`/`dialAndHandshake`/`onStreamLost`/`recover`/`replay`/
`consumePushes`/`stillCurrent`/`currentConn`/`connStatus` 与 `intent`/`deregType`/
`actionFor`/`regKey`：`registry/nacos/nacos.go` 从 638 行降到 348 行，只剩
`Options`/`Expose`/`Watch`/`Unsubscribe`/`Query`/`Close` + watcher 扇出 + 实例转换 +
快照。连接、重连、重放、failover 全在 `nacos/naming_client.go`。

两处语义跟着搬动做了交代：

- `serveStaleOnFailure` 不再自己记订阅意图——`NamingClient.Subscribe` 在**线级调用
  失败时也保留意图**，重连的 replay 会重新订阅，所以快照兜底之后仍会被新列表替换。
  这条保证改由 `NamingClient.Subscribed()` 断言（快照测试原来直接读
  `r.subscriptions`，那是包内私有字段）。
- 重连不再"试 31 次就放弃"。指数退避封顶 30s 之后足够安静，而一次半小时的 Nacos
  故障就把注册中心永久打死，比继续敲门更糟。

最外环的 restart drill 在真 Nacos 上重跑通过：`calls=179 failures=0`、
`state-updates=6`、两轮收敛各约 611ms。这是"生命周期搬家没弄坏收敛性"的读数。

### 第二步还没做

- 收敛导出面（`SetStreamLostHook`/`CloseStream`/`ProbeFatal`/`Unary`/`Call`/`Pushes`/
  `Ack`/`Epoch`/`TrimGroup`）——现在 registry 已不直接碰 Conn，这一步可以做干净了。
- 判据 10：真机开鉴权。
