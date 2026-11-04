# nacos 包 SDK 化 · 第一步（加固）

目标：让 `nacos/` 从"sweet 内部能用"变成"外人读了也敢用"。**不改定位**——仍是
sweet 的内部依赖，不拆 module、不发版。2026-11-03 定案：这不是过渡状态，终点就这样，
所以"敢用"全靠注释与 README 交代，不靠包边界（见文末）。范围收敛与验收判据先落此文件再动代码。

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

1. 不拆独立 module、不发版、不写 CHANGELOG/semver。2026-11-03 定案：这条不是"第二步之后再兑现"，
   就是不做——所以全文里凡是把某件事推给"发独立 module 时"的说法都作废，见文末那节。
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

### 导出面已收敛（同日）

**做法与"逐个方法改小写"不同**：只要 `Dial` 与 `Conn` 类型本身不导出，挂在它上面的
导出方法就不可达，于是收的是类型与构造器——`Conn`→`sdkConn`、`Dial`→`dialConn`、
`NamingCaller`→`namingCaller`、`ServerList`→`serverList`、`ClientVersion` 常量→
`clientVersion`、`TrimGroup`→`trimGroup`（顺带把名字规整挪进 `Subscribe`/`Query`，
registry 不再需要自己剥 `GROUP@@`）。逐个改几十个方法是无收益的 churn。

包外现在只用得到 5 个符号：`NamingClient` / `NewNamingClient` / `NamingOptions` /
`Instance` / `AccessGetter`。

**一个改名踩到的真雷**：`ClientVersion` 同时是包级常量和
`ConnectionSetupRequest` 的 **JSON 字段**，一刀切改成小写后 `go vet` 报
"struct field clientVersion has json tag but is not exported"——字段一旦不导出，
`encoding/json` 直接不序列化它，握手会静默变成"没带 clientVersion"。字段恢复导出，
常量保持小写。

**DTO 决定不删**（`InstanceRequest`/`ConfigQueryRequest`/`ConnectionSetupRequest`
等）。理由：这个包的价值就是"线级契约可读、可逐行对着真服务端断言"，把它们藏进
窄门面等于扔掉那份证据。改在新建的 `nacos/doc.go` 里划清层次——受支持 API 只有两个
客户端 + Options + 返回值类型 + 错误 sentinel；DTO 是协议文档，"别建在上面"。
（这里原本写的是"将来若真发独立 module，把 DTO 挪进 `nacos/wire` 子包，两全"——2026-11-03 定案
不拆 module、不发版，那条路一起作废，层次只剩注释这一道防线，没有包边界替你挡。）

顺带修掉一处过期注释：`envelope.go` 顶着一句"Package conn implements…"——包名是
`nacos`，而且早就不只是传输层了。

### 探针升级成测试（同日）

`registry/nacos/probe_push_test.go` 原来通篇 `t.Logf`、永远不会红。拆成两条：

- `TestPushReachesAnotherConnection` —— 硬断言：一条连接上的订阅者必须收到另一条
  连接注册的推送。这条不成立，全fleet 的 resolver 都是瞎的。
- `TestSameConnectionRegistrationTakesEffect` —— 把两件事分开：注册**生效**用
  `Query` 硬断言；Nacos 是否给发起注册的那条连接也推一份，作为**测量**记录。

真机读数：`same-connection push: YES`。这是原来那个探针想问却没答案的问题。

### 判据 10 已完成（2026-11-03，隔离实例）

**做法**：把 install 复制一份到 `~/ai-infra/nacos-auth-test`（排除 4.7G 日志与 1.3G data，
实得 227M），端口 18848 / gRPC 19848 / console 18080，独立 derby 目录，完全不碰在跑的 8848。
先关鉴权建账号，再翻 `auth.enabled=true` 并把 `token.expire.seconds` 调到 20 秒，好把续期
路径也逼出来。

三条读数：

1. **负向**：不带 `Access` 的 SDK 连接 → 握手**成功**并拿到 connectionId，config 查询被拒，
   错误落进 `ErrDenied`（`denied (auth): query … Code: 401, User not found`）。这同时实测
   印证了"建连不鉴权、每请求才校验"那条源码结论。
2. **正向**：带 `PasswordAccess` 的网关 → `applied 1 route(s)`；越过 TTL 两分钟后再发布 →
   `applied 2 route(s)`，全程不重启。
3. **刷新窗口**：60 秒内成功登录恰好 **3 次** = 预测值（TTL 20s、窗口 ttl/10 ⇒ 每 18s 一次）。
   口令里带 `&`，顺带真机验了一次 form 编码。

`example/gateway` 为此补了 `-username` / `-password` / `-auth-http-addr` 三个 flag——
`PasswordAccess` 此前没有任何真实调用方。HTTP 口显式给，**不做 gRPC↔HTTP 端口互推**。

### 两条运维地雷（都是真踩的）

1. **derby 不种默认用户**：`META-INF/derby-schema.sql:171-175` 只建 `users` 表、没有 INSERT，
   且 HTTP 没有列用户的口。直接 `auth.enabled=true` 会把自己锁在外面。
   `POST /nacos/v3/auth/user/admin` 会**当场生成一个随机口令的 `nacos` 管理员并只回显一次**，
   那是唯一拿到凭证的窗口。
2. **`bin/shutdown.sh:18` 是 `pgrep -f nacos.nacos`** —— 匹配全机所有 Nacos 进程。同一台机器
   上跑第二实例时，关一个会把**另一个也杀掉**。本次就误杀了在跑的 8848（已重启恢复，
   2248 条配置无损，服务列表为 0 是因为那些都是临时实例）。多实例环境下要么按 pid 文件关，
   要么别用这个脚本。

### ConnectResetRequest 已补语义（2026-11-03）

服务端用它把连接赶到别的节点上（过载甩连接、运维 `reloadClient`：
`ConnectionManager.java:286-319`）。官方语义是 **ack 在旧流上、然后换节点**
（`RpcClient.java:397-428`：`switchServerAsync` 只是入队，response 仍从当前连接发出），
不是原地重连——这条推送的目的就是"离开"。实现落在 `nacos/reset.go`：
ack → 选目标（推荐地址须在自己列表内才采纳，且以列表端口为准，`RpcClient.java:319-339`；
无推荐则取下一个；单节点列表就原地重建）→ **先连上新节点再关旧的**（`RpcClient.java:522-544`）
→ **必须重放**（`switchTo` 里显式 replay / re-listen：新连接是健康的，supervisor 不会出手，
不重放就是静默丢注册）。

配套改动：`streamLost(dead)` 加了连接身份判断——计划内切换会关掉一个已被替换的连接，
若照旧回调就会一次事件重连两遍（官方用 abandon 标志做同一件事，`GrpcClient.java:310-322`）。

### 顺手抓出的第二个 ack 类名错误（比 ConnectReset 本身更要紧）

`ackClassFor` 的 `ConnectResetRequest` 分支和 `default` 分支都回 `EmptyResponse`，而
**3.2.3 根本没有 `EmptyResponse` 这个类**（无同名文件、不在 Payload SPI 清单里）。服务端
对未注册类型是 throw（`GrpcUtils.java:132-133`），所以那等于"回了但没被收到"——和上一批
刚修掉的 5743 条 `Unknown payload type` 是同一类错误，只是藏在兜底分支里没被触发过。
正确做法对齐官方：未知推送**不回任何包**（`RpcClient.java:851-878` → `GrpcClient.java:286-290`
只打 warn）。`ConnectResetRequest` 则改回它真正的应答类 `ConnectResetResponse`
（`api/.../remote/response/ConnectResetResponse.java`，SPI 清单第 28 行）。
`TestAckClassIsARegisteredPayload` 里那条"no ConnectResetResponse exists"的注释是错的，已改。

### 一条观测通道的教训

真机验证第一次"看不出结果"：`publish()` 的重连日志是 **debug 级**，而探针的 slog handler
默认 Info 级，于是客户端侧一行都没有——只有服务端 `remote-digest.log` 的
`transportTerminated` 证明事情发生过。放开到 Debug 之后读数完整：

```
03:58:29.562 nacos: config connected  connectionId=1793649509559_127.0.0.1_50571
03:58:33.009 server asked this connection to reset; moving to another node  recommended=:
03:58:33.012 nacos: config connected  connectionId=1793649513010_127.0.0.1_50574
```

即：真 Nacos 3.2.3 发出 `ConnectResetRequest`（无推荐地址分支）→ 客户端 ack → 3ms 后用新
connectionId 重建。测试侧另加六条（`nacos/reset_test.go`）：换节点并重放 listen、旧流上 ack
且 requestId 回显、采纳推荐地址、忽略列表外地址、单节点原地重建、未知推送不 ack、以及
naming 侧注册重放。

### 生命周期骨架已抽取（2026-11-03）

`nacos/lifecycle.go`（452 行）承接两个域共用的部分：连接所有权、`reconnectOnce`、
`publish`、`supervise`、退避、`streamLost`、`switchTo`、`WaitReady` 与全部状态访问器。
`config_client.go` 834→521 行，`naming_client.go` 820→520 行。同日清掉这次搬动留下的孤儿注释后
是 483 / 524（naming 那侧略升是因为把 `Updates()` 的通道契约写全了，见文末）。

域间真正不同的三件事做成钩子：`newConn`（各自的 Options 怎么落到连接上）、
`onReady`（启动谁的推送消费者）、`replay`（重放什么意图）。嵌入而非命名持有，
于是 `WaitReady`/`CurrentAddr`/`ConnectionID`/`Handshakes`/`PushDropped`/`LastErr`
仍是两个客户端自己的公开 API。

**抽取过程改强了一条不变式**：`replay` 原先挂在 `supervise` 和 `switchTo` 两个调用方
身上，而 `start()` 的首次连接不重放——新写的 `TestLifecycleReplaysOncePerConnection`
直接把这条不对称照出来了。现在 `publish` 自己负责重放，语义变成"被发布的连接必然带着
意图"；重放失败就 retire 该连接交给 supervisor，而不是留一条服务端一无所知的健康连接。
调用方忘记重放的可能性被消掉了，这正是抽骨架的意义。

共用测试 5 条（`lifecycle_test.go`，用只握手的 probeStub + 计数钩子）：重试到服务端应答、
每条连接恰好重放一次且空闲时不再重放、`switchTo` 退役旧连接且握手数只 +1（验证被替换
连接的 loss 回调不会引发第二次重连）、`WaitReady` 的错误里带地址与冷却状态、退避增长封顶
加抖动。两个域原有的端到端测试**一行未改**通过，真机 restart drill 复跑
`calls=177 failures=0`、两轮收敛约 562ms，真机 ConnectReset 复跑
`04:49:05.969 收到 → 04:49:05.973 新 connectionId`。

### 这一轮自己踩的两处

1. 批量删函数的脚本按"找下一个 `\n}\n`"定界，**单行方法**（`func (n *X) servers() *serverList { … }`）
   没有独立结尾行，于是一路删到下一个函数，啃掉了 `callSubscribe`/`callQuery`。
   改成按大括号配对定界，并用"HEAD 函数集合 − 当前集合 − 预期删除集 = 空"做审计。
   教训：脚本删代码必须事后做集合差核对，不能只看编译通过。
2. 我以为 Go 禁止在复合字面量里写嵌入结构体的提升字段，准备去"修"一条旧测试——
   实测（最小复现 + `go build`/`go vet` 均通过）**这个工具链是接受的**。
   先验证再动手，别拿记忆当规范。

### 第二步之外还剩什么

判据 1–10 全绿，ConnectReset 已补，骨架已收敛到一处。

**`nacos/wire` 子包作废（2026-11-03 定案）**：它唯一的理由是"发独立 module 时把线级 DTO 挪出去"，
而不拆 module、不发版现在是终点而不是过渡。于是"受支持 API 只有两个客户端 + Options + 返回值 +
sentinel"这条层次**只由 `nacos/doc.go` 那段注释维持**——没有包边界替你挡，想建在 DTO 上的人
拦不住。这也是这个包和"正经 SDK"最实质的差别：SDK 用 module 与版本锁住契约，这里用一段注释。

所以"外人读了也敢用"全部压在文字上，同日按这个口径补了三处交代：

1. README「能力与边界」表此前整张只有命名域，读者会以为这库不做配置——加了配置读侧那一行
   （主张/测试/真机读数三列，读数出自 `doc/nacos-config-routes.md`，并写明写路径不经本库）。
2. `Updates()` 的通道契约写进注释与 `doc.go`：一条通道服务所有订阅（按 `ServiceInfo.Name`
   自己分流）、缓冲 16。当时顺带订正了两处旧注释说错的话——它们写着"丢弃会计数并上报"，而
   `PushDropped()` 与 `OnPushDropped` 盯的是**连接层那条推送缓冲**（`conn.offer`），
   `updates` 溢出既不计数也不回调。**这条不对称下一节就补掉了**，见「第二道缓冲已补计数与回调」。
3. 清掉骨架抽取留下的孤儿注释：批量删函数的脚本把搬去 `lifecycle.go` 的方法的**注释头**剩在了原文件里
   （`config_client.go` 10 块、共 27 行：`budget`/`dialFor`/`reconnectOnce`/`publish`/`WaitReady`/
   `PushDropped`/`Handshakes`/`streamLost`/`ConnectionID`（重复两遍）/`nextBackoff`/`supervise`；
   `naming_client.go` 7 行：`WaitReady`/`Handshakes`/`CurrentAddr`/`PushDropped`/`reconnectOnce`/
   `streamLost`/`switchTo`）。Go 的文档规则是"紧邻函数上方的注释块就是它的文档"，于是 `go doc` 把
   `CurrentAddr` 与 `PushDropped` 当成 `Close` 的说明、把半句 `Handshakes…` 接在 `Subscribed` 前面。
   删之前逐条确认完整版已在 `lifecycle.go` 里（配置域那条独有的"listen 表按 connectionId 建键"
   被 `lifecycle.go` 的泛化说法覆盖，没有丢信息）。`nacos/conn.go` 里 `dialConn` 头顶还写着
   `Dial prepares…`，是导出面收敛那轮的旧名字，一并改。

### 第二道缓冲已补计数与回调（2026-11-03）

`updates`（16 槽）溢出原先只有一条 `LevelDebug` 日志，而默认 handler 是 Info——等于静默丢，
和 E 项「缓冲满不再静默丢」的规矩冲突。那条规矩的理由对命名域一字不改地成立：丢一份
`ServiceInfo`，视图就旧到下次变更。有人会说这是调用方不排空的编程错误、库不该替它计数，但
连接层那条缓冲溢出同样是下游慢造成的，库已经计了——只计一半才是不一致。

补法按最小面来：drop 分支里 `updateDropped.Add(1)`，并把**原始帧**交给已有的
`Options.OnPushDropped`（`push` 就在那个 `for range` 的作用域里，所以零新 API；调用方
`DecodeNotify` 一次就知道是哪个服务掉了）。回调走 `go`，与 `conn.offer` 同一形状——不能同步调，
否则一个卡住的回调会反过来堵住唯一的推送消费者，把第二道缓冲的问题升级成第一道。

**计数没有折进 `PushDropped()`**，这是这轮唯一值得吵的决定。`PushDropped()` 读的是*当前连接*
的数（`lifecycle.PushDropped` → `conn.pushDropped`），换一条流就归零；而这里丢掉的列表在重连
之后**仍然是丢掉的**。两个生命周期不同的数合成一个，注释就得写成"两条缓冲之和，其中一条随
重连归零"——那是把新困惑留给读者。于是多一个访问器：`UpdateDropped()` 挂在 `NamingClient`
上而不是 `lifecycle` 上，因为配置域没有第二道缓冲，挂上去只会给 `ConfigClient` 一个恒为 0 的方法。

三条测试在 `naming_client_test.go` 末尾。**先写测试再实现**：`UpdateDropped` 还不存在时
`go test` 直接编译失败，那个失败就是规格。

- `TestUpdateDroppedIsReported`：不排空灌 24 条，断言**恰好** 8 次丢弃，且回调收到 8 个原始帧、
  `DecodeNotify` 解得出服务名。取精确值而不是"至少一次"，是因为这个消费者从不阻塞、连接层那条
  64 槽缓冲填不上，能丢东西的只有 `updates`。
- `TestUpdateDroppedZeroWhenDrained`：负对照。同样 24 条但一直排空 ⇒ `UpdateDropped()==0`
  且 `PushDropped()==0`。
- `TestUpdateDroppedSurvivesReconnect`：`Kill()` 换一条流之后计数不归零——钉的就是上面那个
  "为什么不折进 `PushDropped()`"。

断言能被证伪这件事单独验过：把 `Add(1)` 改成 `Add(0)`、回调条件改成 `if false` 再跑，
第一条与第三条 8 秒超时变红，负对照仍然绿（正是它该有的表现），然后恢复原状。

### 故意没做的那层：per-service 最新值

更彻底的修法是改语义而不是补报警器。对服务列表来说，队列里存着三份旧列表本来就是错的——
官方客户端也是 per-service 缓存 + 监听读最新。把 `updates` 换成"每个服务只留最新一份 + 一根
len-1 唤醒通道"，溢出丢掉的就是已被更新值取代的旧值，"丢"本身不再是信息损失。

这轮不动它：那要改 `Updates()` 的契约、`registry/nacos` 的扇出，还会碰到那三条每条都买过单的
不变式（重放、`streamLost` 的连接身份、`publish` 内建重放），风险和上面这二十行完全不对等。
记在这里是为了下次别把它当成新发现。

### 关停与投递必须串行（2026-11-04，竞态修复）

**现象。** `go test -race -count=20 ./nacos/` 里 `TestPushDroppedIsReported` 红，失败文本是
`testing.go:1867: race detected during execution of test`（所以耗时只有 0.01s——它不是断言不稳，
是竞态被抽样撞中）。两处栈：

```
Write  sdkConn.Close()  nacos/conn.go:755   ← ConfigClient.Close ← 测试的 defer c.Close()
Read   sdkConn.offer()  nacos/conn.go:493   ← sdkConn.readLoop(:327) ← openStream(:279)
```

**根因。** `offer` 的锁纪律断在检查与发送之间：拿 `pushMu` 读 `closed`、**放锁**、再
`c.pushes <- p`；而 `Close` 在同一把锁下 `close(c.pushes)`。于是 close 与 send 之间没有
happens-before。这是上一轮把 `Conn.closed` 改成 `atomic.Bool`（双锁等于没锁那件事）之后**剩下的
另一半**——标志位原子了，检查与发送仍然跨锁。

**比竞态本身更值得记的一条。** 那个窗口真跑起来是 `panic: send on closed channel`，而
`readLoop` 头上有 `defer c.guard("readLoop")`，于是 panic 被吞成"丢流"→ 重连 → 重放。也就是说
一次正常关停会伪装成一次没来由的连接抖动并丢掉一条推送。竞态检测器报出来，反而是这件好事。

**修法。** 发送留在 `pushMu` 里（`select/default` 永不阻塞，所以持锁不花钱），`dropped` 出锁之后再
计数与回调。`Close` 与 `offer` 由此串行：要么 offer 看见已关闭直接返回，要么发送先完成再 close。

**测试。** 新增 `nacos/offer_close_test.go` 的 `TestOfferVersusCloseIsSerialized`：8 轮，每轮
`dialConn` 出一条真连接，16 个 goroutine 各投 200 发，`Close` 并发落在中间。带一个排空者
`for range c.Pushes()`——**没有它，绝大多数 offer 会走 `default` 分支（缓冲满），而那条分支本来就不会
竞争，测试就测不到出事的那条路**。末尾再补 100 发"关闭之后投"，钉住契约是空操作而不是 panic。

证伪与实证都过了：修前这条测试 0.836s 必红，报出的正是同一签名的 `DATA RACE`（`Close` 于 `:755`
对 `offer` 于 `:493`）；修后 `-race -count=10` 全绿，原先那条 `TestPushDroppedIsReported` 在
`-count=20` 下也不再冒红。窗口宽度没有量——它是调度决定的，能钉的是"不再重叠"。

### WaitReady 的语义收紧（2026-11-04）

**根因。** `publish` 原来在锁内就 `broadcastLocked()`，出锁之后才 `onReady` 与 `replay`。于是
`WaitReady` 会在意图还没落上去时就返回 nil——"被发布的连接必然带着意图"这句话在 `WaitReady`
返回的那一刻并不成立。`TestLifecycleKeepsRetryingUntilTheServerAnswers` 那 2%–3% 的红就是这一条
的抽样表现（它假定 `WaitReady` 回来时钩子已经跑过）。基线数字：`e18376f` 9/480、`41db317` 4/240、
`122e5ab` 7/240、本次改动前 8/240——**早于今天所有改动**，中途我怀疑过是 Tier 1 引入的，扩样之后
被自己的数据推翻（`e18376f` 那个 0/240 是运气）。

**做法。** `lifecycle` 加一个 `published`：连接成为 current 时置 false，`onReady` + 一次返回 nil 的
`replay` 之后置 true；`streamLost` 与 `closeConn` 清零。`WaitReady` 的谓词改成
`published && conn != nil && conn.Ready()`；广播挪到最后，并加身份检查 `l.conn == conn`，
免得被取代的 generation 给自己发凭证。replay 失败时不广播，直接 retire 交给 supervisor 重试。

**为什么连接要提前 current。** `replay` 自己要用 `liveConn()` 把注册与重订阅发出去，所以
"能用"必然早于"可担保"。`liveConn()` 因此**保持宽松**——它是"我现在能不能发请求"的门，
不是"意图都落了吗"的门；后者是 `WaitReady` 的职责。把两者混成一个谓词会直接把 replay 锁死。

**影响面。** 非测试调用点只有 `example/gateway/main.go:228` 的 `cfg.WaitReady`。config 的 replay
在没有 watcher 时走 `listen(nil)`，`config_client.go:335` 直接返回 nil，所以不会自锁或卡启动。

**测试（规格先行）。** 新增两条，都在改之前 0.00s 必红、之后 `-count=2` 绿：

- `TestWaitReadyWaitsForReplay`：把 replay 钩子扣住不放，`WaitReady` 必须超时返回错误。
  扣住钩子而不是等计数器，是为了把断言变成**顺序**问题而不是赛跑问题。
- `TestWaitReadyDoesNotReportReadyWhenReplayFails`：第一次 replay 注定失败并被扣住，`WaitReady`
  不得为它发凭证；释放后必须成功，且 `replays ≥ 2`、`currentConn()` 仍 Ready。

**收益可量化。** 那条老 flake 归零：`-race -count=40` 连跑六轮 = 240 次迭代，0 失败。

**没做的。** 真机最外环没跑——这条改的正是重连重放的时序，按纪律单元测试替代不了它。
restart drill 与 ConnectReset 都需要你那套 8848 加人工重启，等你安排。
（下一节把这些读数补上了。）

### 真机最外环已跑（2026-11-04，本地 Nacos 3.2.3 单节点 9848）

收紧之后补的外环读数，全部取自同一次运行、同一台服务端（pid 49060，
`nacos.core.auth.enabled=false`、`nacos.core.auth.admin.enabled=false`）。

**① rolling restart drill**（`registry/nacos/restart_drill_test.go`，provider 由测试自己起停）：

```
round 0: took down 21001, converged to the survivor 21002 in 561.135542ms
round 1: took down 21002, converged to the survivor 21003 in 557.764917ms
calls=175 failures=0 ([]) state-updates=6 convergence=[561.135375ms 557.764917ms]
```

`-race` 开、PASS 5.67s。`failures=0` 是不漂的那半，`calls` 随运行次数变。

**② `-run Live` 六条**（握手能力表、一元 ServerCheck、setup 不被调用方超时拆掉、探针通道事实、
注册/查询/注销、订阅种子与推送）：对真服务端全 PASS。

**③ ConnectReset**（服务端赶连接 → ack → 换节点 → 重放）。触发方式记下来，下次不用重新找：
`POST /nacos/v3/admin/core/loader/reloadClient?connectionId=<SDK connectionId>`
（`ServerLoaderControllerV3.java:117`，类级映射是 `NACOS_ADMIN_CORE_CONTEXT_V3 + "/loader"`）。
**POST 必须带 Content-Type**——`http.Post(url, "", nil)` 会被 Spring 顶回来：
`Invalid mime type "": 'mimeType' must not be empty`。单节点列表下 `redirectAddress` 为空 ⇒ 原地重建。
读数（毫秒级，同一次运行）：

```
21:02:25.526 connected  connectionId=1793797345523_127.0.0.1_62438   replayed registrations=0
21:02:25.527 registered: server sees 1 host(s) after 0s
21:02:25.532 server asked this connection to reset; moving to another node  recommended=:
21:02:25.536 connected  connectionId=1793797345534_127.0.0.1_62440
21:02:25.537 replayed registrations=1
21:02:25.541 WaitReady after switch returned in 0s; server hosts=1 instance-present=true
```

一句限定：`WaitReady after switch returned in 0s` **不能**当作"收紧生效"的证据——问的时候切换已经
完成了。把顺序钉死的是单元测试里那条扣住 replay 钩子的断言（改前 0.00s 必红）。这里读到的是"真服务端
赶连接时，注册确实被重放到新连接上、服务端仍列着它"。

**④ 撞出来的一个真缺口：namespace 空值整个不发。** `NamingOptions.Namespace` 的注释写着"Empty namespace
means public"，但真 3.2.3 上第一次 `Register` 就被拒：

```
nacos: request refused: register registerInstance sweet.example.v1.ResetProbe
(errorCode=500): Cannot invoke "String.equals(Object)" because "this.namespace" is null
```

机制在 `naming_types.go:51/63/75/110`——`Namespace string json:"namespace,omitempty"`，空值被
`omitempty` 整个省掉，服务端那个字段是 null 而不是空串，于是 NPE。复现 2/2。
`registry/nacos` 之所以一直没撞上，是它自己在 `nacos.go:102-103` 把空 Namespace 兜成了 `"public"`——
**适配层补的洞，库层没补**，而库的注释还把"空＝public"当契约写给了外人。

**定案与修法（2026-11-04）：库层兜成 `"public"`，两个域一起做。** 依据不是我们挑了一个写法，而是
官方客户端就是这么做的：`InitUtils.java:86-87` 把空 namespace 兜成 `UtilAndComs.DEFAULT_NAMESPACE_ID`
（`:40` = `"public"`），`ClientBasicParamUtil.java:148-149` 对 config 的 tenant 做同一件事
（`Constants.java:191` 也是 `"public"`），然后 `NamingGrpcClientProxy.java:280` 等把这个值原样放进每个
请求。所以另外两个候选被证据否掉：**去掉 `omitempty` 发空串**＝造一个官方客户端从来没走过的形状，
而跨语言互通是这个包的主证；**改注释要求调用方显式给**＝把官方已经替用户做的事推给外人，还会让
`registry/nacos` 现有的用法（不带 namespace）变成"文档说能用其实不能用"。

落下来的四处：`NewNamingClient` 在 `GroupName` 默认旁边加 namespace 默认（两者同构）；
`NewConfigClient` 同样兜 `Tenant`；`registry/nacos` 那两行兜底删掉（库兜了它就是死代码，注释留一句
说明它以前是适配层在替库做事）；两个 Options 的注释改成实况。

**桩不忠实这一条也顺手补了。** 改完 `TestConfigWatchPullsAfterNotify` 红了一句"no update after the
notify"——不是客户端坏了，是桩在教错的形状：它把脚本化的 notify 原样发出，`Tenant` 留空，而真服务端
回的是**监听者自己上报的那个 tenant**（`NamespaceUtil.java:44-49` 把空串规范成 `namespaceDefaultId`，
`AsyncNotifyService.java:170` 发 `task.getTenant()`）。客户端的 watcher 键现在是 `"public"`，
推来的却是 `""`，查不到就不回调——桩如果不忠实，"测不出来"会一路伪装成"没坏"。改成：脚本没给 tenant
时，桩从已记录的 listen 里取该键的 tenant 填上（`config_test.go` 的 `tenantListenedFor`）。
`TestConfigQueryRoundTrip` 里那句 `got.Tenant != ""` 也跟着改成 `"public"`——它原先断言的正是这个错形状。

**真机闭环（同一天，同一台 3.2.3）**：改前用空 namespace 跑那个探针是 `errorCode=500` NPE（2/2）；
改后同一条命令 `NAMESPACE=`（空）跑通——`21:52:31.707 registered: server sees 1 host(s)`，同一次运行里
ConnectReset 也再走了一遍（`.037` 新 connectionId、`handshakes=2`、注册重放回来）。单元测试这边
两条请求形状断言先红后绿：`InstanceRequest.namespace == "public"`、请求体里必须出现
`"tenant":"public"`（改前桩看到的 body 是 `{"dataId":"routes.json","group":"DEFAULT_GROUP"}`，
`tenant` 这个键根本不存在——那条红字就是缺口本身的形状）。

**config 的真机 Watch→notify 全链路也跑了**（这条桩证明不了——回显 tenant 的正是桩本人）。经 v3
admin 口发布 `DEFAULT_GROUP/sweet-tenant-probe.json`（`namespaceId=public`），客户端不设 `Tenant`，
`Watch` 之后再改一次表：

```
+0.0s callback: content={"v":1} md5=a191475ae2bf7db9c7e320f7da455bbb tenant="public"
+0.0s published v2; waiting for notify → pull → callback
+0.1s callback: content={"v":2} md5=554a7f6757030bc85dc45e129076ef94 tenant="public"
OK: 2 callbacks; the watcher keyed on tenant="public" was found by the server's notify
+0.2s query a missing key -> ErrNotFound=true
+0.2s cleanup: probe config deleted
```

两个读数值得单独记。`tenant="public"` 是**服务端回给客户端的**，不是我们填的——这正是桩必须改成
回显监听者 tenant 的原因；seed 那行的 md5 与 v3 admin 口读回的 `a191475ae2bf7db9c7e320f7da455bbb`
逐字相同，两边算的是同一个 MD5。探针自己踩了一次坑：发布后立刻查会拿到 not found——服务端的发布
与读缓存不是同一瞬间，第一版探针就死在这儿（那是探针在和服务端赛跑，不是客户端 bug）；改成先经
admin 口按 md5 确认 v1 可读再起 Watch。v2 那一步**故意不等**可读再等回调：notify 是发布那一刻发出的，
先等可读会让客户端的回拉撞上服务端还没 dump 完，把唯一一次 notify 吞掉。

跑完善后核过：探针配置已删（admin 口回 `Config not exist`），三个探针服务
`ResetProbe`/`RestartDrill`/`RollingDrill` 在实例表里都不在了（临时实例随连接消失）。

**⑤ `TestReconnectAfterServerRestart`：先发现它是假绿，再把它修成能绿的。**

原测试（`registry/nacos/persist_probe_test.go`）只等"实例还在"：`Expose` 之后每 5 秒 `Query` 一次，
只要返回同一个 port 就判 recovered。可是什么都不重启时注册当然还在——实测它 **5.02s 就 PASS**
（Nacos 进程 `etime` 未变、9848 一直在听，作为对照证据）。也就是说这条测试无法区分"服务端把连接
拿走之后客户端重放成功"与"什么都没发生"，而 README 拿它当"服务端重启后重连并重放"的钉子——那根
钉子当时是空的。人手动重启最快也要十几秒，测试早就返回了，所以"由人重启"这个流程本身也保证不了
重启落在断言窗口里。

改成两段，第一段必须先**观察到消失**（`Query` 报错或返回 0 个实例；每次探测用自己的短 ctx，
否则共享 ctx 的 deadline 会和"消失"长得一模一样；`context.DeadlineExceeded`/`Canceled` 明确不算
消失），第二段才等它带着同一个 port 回来——期间没人再调 `Expose`，所以回来只能是重放。
证伪跑过了：**不重启时修后的测试 FAIL 在 180.32s**，文本就是"never observed the registration
disappearing — the server was not restarted, so this run cannot tell a successful replay from
nothing happening"。

真跑（2026-11-04，同一台 3.2.3，`-race`）。重启用的是安全姿势：`kill <pid>`（SIGTERM），
**不碰 `bin/shutdown.sh`**——它那句 `pgrep -f nacos.nacos` 会全机杀 Nacos；启动前先 `cd` 进安装目录，
否则 `derby.log` 与 `logs/` 会被写进当前仓库根目录。时间线：

```
21:20:30  SIGTERM -> pid 49060        21:20:32  进程退出、9848 closed
21:20:32.189  loss observed: query failed: nacos: server unavailable ... on cooldown
              (last error: ... transport: Error while dialing: dial tcp 127.0.0.1:9848: connect: connection refused)
21:20:40  9848 open again（新 pid 71728，中断约 10 秒）
21:20:48  recovered 16s after the loss: registration replayed automatically
          (instances=[{127.0.0.1 50015 100 map[]}])
--- PASS: TestReconnectAfterServerRestart (44.08s)
```

重启后另跑了一遍 `-run Live` 六条确认服务端健康（`nacos` ok 4.590s、`registry/nacos` ok 2.746s），
drill 起过的 provider 进程与端口都无残留。derby 里的配置这次没数（上次核过 2248 条无损），
如果要精确核对得走 v3 管理口读一遍——本轮没做，标在这里。
