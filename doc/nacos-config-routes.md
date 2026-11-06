# 网关路由外置：Nacos 配置域客户端（手写）

范围说明与验收判据。文中“本轮”“冻结”指 2026-11-02 的配置域客户端阶段；后续网关转码升级
已在 `doc/grpc-gateway-adaptation.md` 中单独决策，历史验收记录保留，不伪装成新做的真机测试。

## 目标

sweet 的 HTTP→gRPC 网关最初每条路由都在 Go 里声明（`gateway/gateway.go` 的 `Route`
带 `NewRequest`/`NewReply` 两个闭包）。配置域阶段把**策略**（HTTP 方法/路径 → 注册中心查找名
→ gRPC 方法、是否服务端流）外置到 Nacos，**类型**仍留在 Go。后续升级支持从进程内已编译 descriptor
或 `Options.DescriptorFiles` 自动取得类型与流式基数，因此通常连 `Options.Services` 消息工厂表也不必填写。

两种方式都不要求 provider 开启 reflection。已编译模式要求 Proto Go 包链接进网关；文件模式使用二进制
`FileDescriptorSet`，可以引入当前二进制从未链接过的 service。

## 契约锚点（nacos @3.2.3，本地 checkout，`git describe` = 3.2.3）

- 线格式：`Payload{metadata, body=Any}`，`Metadata.type` = Java 类**简单名**，body = 该对象
  的 JSON 且**不含 headers**（`GrpcUtils.java:53-113`）。sweet 已实现（`nacos/envelope.go`）。
- config 域请求/响应：`ConfigQueryRequest{dataId,group,tenant,tag}` /
  `ConfigQueryResponse{content,md5,contentType,encryptedDataKey,lastModified,tag,beta}` /
  `ConfigBatchListenRequest{listen,configListenContexts[{dataId,group,tenant,md5}]}` /
  `ConfigChangeBatchListenResponse{changedConfigs[{dataId,group,tenant}]}` /
  `ConfigChangeNotifyRequest{dataId,group,tenant}` → ACK `ConfigChangeNotifyResponse`。
  基类 `Response{resultCode=200,errorCode,message,requestId}`。
- **监听立即返回，没有 30s 挂起**：`ConfigChangeBatchListenRequestHandler.java:60-96`
  同步返回「哪些 md5 已过期」，gRPC 模式用推送替代长轮询。
- **推送只带 key，正文永远由客户端拉**（`jaws/doc/nacos-config.md:79`）：收到
  `ConfigChangeNotifyRequest` → 回 ACK → 发 `ConfigQueryRequest` → 更新本地 md5 →
  用新 md5 重发 batch listen（服务端按 connectionId 记 md5，见 :80 `addListen`）。
- **ACK 必须回填 requestId**：`GrpcClient.java:282-285` + `RpcAckCallbackSynchronizer`，
  否则服务端按 `tryTimes*2` 秒重推、超限 unregister 本连接。
- **naming 与 config 是两条连接，不共用**：`NamingGrpcClientProxy.java:116` 打
  `module=naming`、`ClientWorker.java:752-755` 打 `module=config`、
  `RpcClientFactory.java:126` 按 clientName 各建一个 `GrpcSdkClient`，且服务端
  `ConnectionBasedClientManager.java:60-64` 对 `module != naming` 的连接**直接 return**。
  「与 listen 复用同一条 SDK 面连接」（`jaws/doc/nacos-config.md:31`）说的是 config **域内**。
- 鉴权：config 的 handler 带 `@Secured(signType=CONFIG)`，凭据走 `Metadata.headers`
  的 `accessToken`（或 `username`/`password`），**每请求校验**；未开鉴权时跳过
  （`RemoteRequestAuthFilter.java:84-86`）。

## 原阶段不做（历史范围）

1. 不引 `nacos-sdk-go`；不引 YAML/TOML——配置格式用 JSON + `encoding/json`。
2. 原阶段不做 reflection / descriptor 动态消息；后续已实现**进程内已编译 descriptor**和二进制
   `FileDescriptorSet` 文件，仍未实现服务端 reflection。
3. 不实现写回：`ConfigPublishRequest` / `ConfigRemoveRequest` 不做，只读。
4. 不做 fuzzy watch、灰度(beta) 配置、`encryptedDataKey` 解密、多 dataId 聚合。
5. 原阶段不改路由匹配语义；后续仅增加 `{field}` / `{a.b}` 单段路径变量，仍不做 `*`、`**`
   通配或前缀匹配。
6. 不动 naming 侧任何行为；不给 config 与 naming 共用连接（官方即两条）。
7. 新增文档不挂门面 README。

## 配置格式

dataId 默认 `sweet-gateway-routes.json`，group `DEFAULT_GROUP`，命名空间走 `-namespace`：

```json
{"routes":[
  {"method":"POST","path":"/v1/greeter/greet","service":"greeter-provider",
   "fullMethod":"/sweet.example.v1.Greeter/Greet","body":"*"},
  {"method":"GET","path":"/v1/greeter/{name}","service":"greeter-provider",
   "fullMethod":"/sweet.example.v1.Greeter/GreetStream","body":"","responseBody":"message"}
]}
```

`service` 是注册中心查找名（与 gRPC 服务名可以不同，jaws 服务就是 `jaws/<FQN>`）；消息类型
优先取 `Options.Services` 显式覆盖，其后依次查 descriptor 文件和已编译 descriptor，流式基数也自动推导。
`body` 缺省兼容旧表的整请求体行为，`"*"` 显式整包，`""` 仅 path/query，字段路径则绑定局部 body；
`responseBody` 选择响应字段。所有字段路径在换表前校验，失败仍整批拒绝。

### Descriptor 文件

`Options.DescriptorFiles`（示例命令的 `-descriptors`，逗号分隔）配置网关本地的二进制
`FileDescriptorSet` 路径。生成方式：

```sh
protoc -I example/greeter --include_imports \
  --descriptor_set_out=/tmp/sweet-greeter.pb greeter.proto
go run ./example/gateway -routes=nacos -descriptors=/tmp/sweet-greeter.pb
```

每次 `ApplyRoutes` 都重新读取所有文件，构建私有 registry，再解析并校验整张路由表。文件定义的 service
使用 `dynamicpb`，优先于同名的已编译定义；文件没有的 service 才回退到已编译 descriptor。文件不会注册进
全局 registry。新增 service 时，将新 descriptor 部署到已经配置的文件路径，再发布或重放路由即可，无需
重新编译网关；新增一个启动时未配置的文件路径仍属于进程配置变更。

实现不 watch 文件系统。运维上应先写临时文件再原子 rename，然后触发路由应用，避免读取到半份文件。
文件缺失、依赖不完整、定义冲突或路由校验失败都会整批拒绝，线上表不变。路由缓存不内嵌 descriptor，
冷启动恢复时这些文件必须仍然存在且与缓存路由兼容。

## 关键语义决定

- **坏表整批拒**：任一条路由解析不出类型或校验失败 → `ApplyRoutes` 返回 error，
  线上表**一字不动**（不出现半套路由）。
- 拉取失败/解析失败 → 保留上一次好表，错误返回给调用方记日志。
- 热更新只换「匹配表」，已建立的 gRPC 连接按查找名继续复用，不因换表而断开。
- 断连重连后必须用**当前 md5** 重放 batch listen（服务端订阅表按 connectionId 存，
  旧 connectionId 的订阅随断连失效）。
- **配置中心不可达不得杀死冷启动**：`NewConfigClient` 对首次建连失败只报告不拒绝
  （返回可用客户端 + 后台重试），`Watch` 也照样登记，种子拉取失败返回的是提示性错误。
  理由与 `registry/nacos/snapshot.go` 的 `scheduleRecover` 同源——否则下面的缓存永远
  轮不到出场。
- **路由缓存**（`Options.RouteCacheFile` + `RestoreRoutes`）：只在成功换表**之后**落盘，
  所以缓存里永远不会出现被拒的表；读回时校验文件内 `source`、拒装无时间戳 / 空表 /
  已解析不出的表；默认关闭，与 registry 快照目录同一套开关纪律。启动顺序是先 restore
  再 watch，live 表到达后覆盖旧的、并把 age 如实打进日志。

## 验收判据（可当场跑）

1. `go build ./... && go vet ./... && go test ./...` 全部 rc=0。
2. `TestConfigQueryRoundTrip`：桩服务端收到 `ConfigQueryRequest`，断言 type 串与
   body 字段名逐字对齐（`dataId`/`group`/`tenant`），响应 md5/content 被解出。
3. `TestConfigWatchPullsAfterNotify`：桩推 `ConfigChangeNotifyRequest{requestId:"7"}` →
   断言同一条 bidi 上出现 `ConfigChangeNotifyResponse` 且 `requestId=="7"`；随后出现
   `ConfigQueryRequest`；回调拿到新 content；**且**用新 md5 重发了一次 listen。
4. `TestConfigReplayAfterReconnect`：桩在握手后丢流 → 重连成功 → 断言桩收到第二次
   listen，其 `md5` 等于重连前拉到的值（不是初始空串）。
5. `TestApplyRoutesRejectsBadTable`：表里塞一条未知 `fullMethod` → 返回 error，且
   `Routes()` 与改前逐字相同。
6. `TestApplyRoutesSwapsLive`：挂在真 HTTP server 上（非 ResponseRecorder），换表后
   旧 path 404、新 path 200。
7. 真机：本地 Nacos 3.2.x 起 example/gateway `-routes=nacos`，**不重启网关**改配置，
   新 path 立刻可调用；输出块取自同一次运行、同一端口。
8. `TestConfigClientHealsWhenCentreAppears`：先占一个端口再释放，客户端对着"没有服务端"
   的地址构造成功（`err` 报告首次握手失败但客户端可用）；`Watch` 登记后返回提示性错误、
   不回调；随后把桩服务端绑到同一地址，断言**没有人再调用 Watch**，内容自己到了。
9. 缓存五条：`TestRouteCacheRoundTrip`（换进程从盘上装回并可服务）、
   `TestRouteCacheNotWrittenOnFailedApply`（被拒的表不落盘）、
   `TestRouteCacheRefusesMismatchedSource` / `...UndatedAndEmpty` /
   `...TableThatNoLongerResolves`（三种拒装）、`TestRouteCacheDisabledByDefault`、
   `TestRestoreRoutesWithoutFileIsNotAnError`。
10. 真机兜底：网关 #1 连真 Nacos 写盘 → 停掉；网关 #2 用 `-server` 指向死端口 +
    `-registry=direct` 保住后端，断言从缓存装上路由并返回 200，且日志如实报 age。

## 验收结果（2026-11-02，全部达成）

1–6 在 `go test -race ./nacos/ ./gateway/` 下全绿；模块全量 `go build ./... && go vet ./... &&
go test ./...` 亦全绿。测试名与判据一一对应：`TestConfigQueryRoundTrip`、
`TestConfigWatchPullsAfterNotify`、`TestConfigReplayAfterReconnect`、
`TestListenResponseStaleSubsetIsPulled`、`TestConfigClientIsLabelledAsConfig`、
`TestAckClassIsARegisteredPayload`、以及 `gateway` 侧五条 `TestApplyRoutes*` /
`TestParseRouteSpecs*`。

7 真机（本地 Nacos 3.2.3，`127.0.0.1:9848`，一次运行、端口 20112、全程未重启）：

| 动作 | 观测 |
| --- | --- |
| 发布 1 条路由 | `applied 1 route(s) … (md5 fe8fdd267ca343d4b34eeced92392192)`，与 v3 管理口读回的 md5 逐字相同 |
| `curl /v1/greeter/greet` | `{"message":"Hello, web!"}` |
| 发布 2 条（加流式） | 2s 后 `applied 2 route(s)`，`curl -N` 逐帧三行 |
| 发布坏表（方法未注册） | `apply failed: … not registered (known: Greet, GreetStream) — live table untouched`，两条旧路由仍 200 |

补跑（同日 21:02，端口 20113，仍未重启）验的是**删除**方向，前一次只验了新增：

| 动作 | 观测 |
| --- | --- |
| 启动时配置不存在 | `no route table yet … the edge answers 404 until one is published`，进程照常起来 |
| 发布表 A（只有 greet） | 2s 后 `applied 1 route(s) (md5 fe8fdd…)`；greet 200、greet-stream 404 |
| 换成表 C（只有 greet-stream） | 再 2s，`applied 1 route(s) (md5 165d7d…)`；**greet 404、greet-stream 200** |

这一跑顺带钉住另一件事：配置从"不存在"到"首次发布"也能被收到——Watch 在 not-found 时以空 md5
登记订阅，发布触发 notify → 回拉 → 装上，不需要重启或重试。

补记一条实现约束：Nacos 3.x 的 v1 HTTP 口已移除（`No endpoint POST /nacos/v1/cs/configs`），
发布走 `/nacos/v3/admin/cs/config`，参数名是 `groupName` 而非 `group`。

8 与 9 在 `go test -race -count=1 ./...` 下全绿。10 真机（同日 21:35，缓存文件 295 字节，
网关 #2 的 `-server` 指向无人监听的 19999、后端走 `-registry=direct`）：

```
2026-11-02 21:35:20 route cache: restored 1 route(s) (age 1m0s) from …/routes.cache.json — stale until the config centre answers
2026-11-02 21:35:20 config centre 127.0.0.1:19999 unreachable at startup: nacos: server check 127.0.0.1:19999: rpc error: code = Unavailable … connection refused — retrying in the background
2026-11-02 21:35:20 watch DEFAULT_GROUP/sweet-gateway-routes.json: nacos: call ConfigQueryRequest: … connection refused — registered, will pull once the config centre answers

$ curl -s localhost:20116/v1/greeter/greet -d '{"name":"still-cold"}'
{"message":"Hello, still-cold!"}
```

## 验收过程中修掉的三处

1. **`NewConfigClient` 原本对着不可达的服务端直接返回 error**，example 会 `log.Fatalf`。
   那等于"配置中心挂了 → 网关起不来"，缓存写了也白写。改成报告但不拒绝，后台重试，
   `Watch` 照常登记（判据 8 就是钉这个）。
2. **桩服务端不忠实**：`ConfigBatchListenRequest` 一律回空 `changedConfigs`，而真服务端
   是按 `isUptodate(groupKey, md5, …)` 比对后回"哪些过期"
   （`ConfigChangeBatchListenRequestHandler.java:82-88`）。判据 8 第一次跑不绿就是被这个
   假桩挡住的——**测不出来不代表没坏，也可能是测量装置自己不对**。
3. **一句假日志**：从缓存装回 1 条路由之后，最后一行仍说
   `no route table installed yet … answers 404`，因为那个标记只跟踪 live watch。
   改成按 `len(gw.Routes())` 判断。上面 21:35 的日志块已是修正后的版本；
   更早那次（21:02 补跑，判据 7 的表格里）引用的 `no route table yet …` 是修正前的原文。


## 顺手修掉的一个真 bug：Ack 的类名

`nacos/conn.go` 的 `Ack` 原本用 `p.Type + "Response"` 拼应答类名，对
`NotifySubscriberRequest` 拼出 `NotifySubscriberRequestResponse` —— 官方类是
`NotifySubscriberResponse`。服务端 `GrpcUtils.parse`（:132-133）对未注册类型是 **throw**，
所以这个 ACK 从来没进 `RpcAckCallbackSynchronizer`，等价于没 ACK。

不是推断，服务端日志有账：`ai-infra/nacos/standalone/nacos-3.2.3/logs/remote-digest.log*` 里
`Unknown payload type:NotifySubscriberRequestResponse` 共 **5743** 条（10-26 起累积，当天 5682 条）；
本轮改用显式映射后，20:46 之后新条目 **0** 条，且 `Unknown payload type:Config*` 始终 **0** 条。

为什么之前没被发现：现有 naming 测试只断言"投递发生了"，没断言"服务端收到了什么"——
观测通道上根本没有读数，绿灯就照常交了。所以本轮把类名单独钉成一张表
（`TestAckClassIsARegisteredPayload`），并把 `ConnectResetRequest → EmptyResponse` 这种
字符串规则推不出来的情形写进注释。

## 原阶段冻结结论

上述配置域阶段达成后冻结的是：不加 fuzzy watch、不加写回、不把 config 与 naming 合并成一条连接。
后续 descriptor 与 HTTP 绑定升级没有改变这些边界；配置写路径仍由 Nacos admin API 或控制台负责。

