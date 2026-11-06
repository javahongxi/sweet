# Nacos 不可达时的快照回落

sweet 的实例快照回落用于撑过注册中心冷启动不可达的空窗期，**默认关闭**。它不会绕过注册中心；Nacos 恢复后会自动重订阅，并重新回到以服务端视图为准。

## 行为边界

进程**运行中**断连：重连后重放注册与订阅意图，这条路有测试覆盖，见[测试与能力验证](testing.md)。

进程**新启动**时 Nacos 不可达：默认仍然拿不到实例列表、直接失败——`Options.SnapshotDir` 不设时，sweet 一个字节都不读也不写。

设置 `SnapshotDir` 才开启盘上回落。

## 回落规则

规则刻意比两套官方客户端都紧：

- 只在**冷拨号或订阅失败时**才用快照，正常路径永远以服务端为准；空列表一律不落盘——一次抖动的空答案不该固化成"谁都不许连"。
- 服务名放进文件名装不下（`jaws/xxx` 这种带 `/` 的名字），所以文件里带 `service/group/namespace` 三元组，读时对不上就拒绝——拿别人的实例列表比拿不到更糟；没有时间戳的列表也拒绝，年龄是这条路的全部意义。
- 陈旧必须显式可见：日志一行 WARN，同时回调 `OnSnapshotFallback(service, instances, age)`，另有 `SnapshotAge(service)` 给调用方和测试直接问。
- 回落后订阅意图保持 live 并启动重连循环，Nacos 恢复时重放会重新订阅、用新列表替换旧列表。它撑过的是空窗期，不是"绕过注册中心"。

## 与官方客户端对照

对照过 Nacos 的两套官方客户端，它们并不一致，所以别把任何一套当成"Nacos 本来就这样"：

- **Java `nacos-client`（3.2.3）**：`ServiceInfoHolder` 构造时可以从盘上预载实例表（`client/.../naming/cache/ServiceInfoHolder.java:67-71`；`getServiceInfo` 只读这份内存 map，`:105-109`），所以 server 全挂时重启仍能拿到上次落盘的列表。但预载开关 `NAMING_LOAD_CACHE_AT_START` **默认 false**（`:81-89`），且救的是查询类调用——`doSubscribe` 只在重复订阅时用当前缓存通知（`NacosNamingService.java:739-746`），随后照旧发 RPC、失败照旧抛（`:546`）。运维侧另有放文件即生效的 `FailoverReactor`（`backups/FailoverReactor.java` + `DiskFailoverDataSource`）。
- **Go `nacos-sdk-go` v2.3.5**：预载默认是开的（`naming_cache/service_info_holder.go:54,60-61`），但那份缓存没有 TTL、也没人校验 `Checksum`；运行时 `Subscribe`/查询失败一律 `return err` 不回落（`naming_grpc/naming_grpc_proxy.go:176-189`）；注册意图的 redo 队列是纯内存 `ConcurrentMap`（`connection_event_listener.go:33-34`），进程重启即清零；命名侧的 failover 至今是那句 `//todo FailoverReactor`（`service_info_holder.go:110`）。

## 已验证范围

`registry/nacos/snapshot_test.go` 六条用例钉住规则，含负对照——同一个不可达的服务端，不配 `SnapshotDir` 时必须仍然失败。另有一条**真的把本机 Nacos 停掉**的冷启动端到端，provider 全程在线，所以"回落后能不能调通"是实测而不是推断。

不带 `-snapshot` 的冷启动直接失败，错误链原样交给调用方：

```text
Greet: nacos: subscribe sweet.example.v1.Greeter: nacos: handshake with 127.0.0.1:9848: nacos: server check 127.0.0.1:9848: rpc error: code = Unavailable desc = connection error: desc = "transport: Error while dialing: dial tcp 127.0.0.1:9848: connect: connection refused"
```

带 `-snapshot` 的同一场景，先声明陈旧，再四种形态全部调通；Nacos 在 hold 期间被拉起，hold 结束那一发仍走通了：

```text
serving sweet.example.v1.Greeter from snapshot: 1 instance(s), 0s old — addresses may be stale
resolved sweet.example.v1.Greeter through nacos registry
Greet       <= "Hello, sweet!"
GreetStream <= 1 "Hello #1, sweet!"
GreetStream <= 2 "Hello #2, sweet!"
GreetStream <= 3 "Hello #3, sweet!"
GreetStream == 3 frames, closed by server
GreetChat   <= 1 "Hello, a!"
GreetChat   <= 2 "Hello, b!"
GreetChat   <= 3 "Hello, c!"
GreetChat   == 3 frames from 3 requests, closed by server
GreetAll    <= "Hello, a, b, c!"
GreetAll    == 1 frame from 3 requests
holding 80s: restart Nacos now to watch the snapshot fallback heal
after hold  <= "Hello, sweet!"
```

恢复也不靠重启进程：那份快照的 `savedAt` 从 `2026-10-28T06:03:43.369088+08:00` 被重订阅后的新列表刷成 `2026-10-28T06:04:05.001761+08:00`。`0s old` 是因为这条端到端里快照是同一次脚本几十秒前刚播种的，真出故障时这里是分钟级。

一句话总结：回落撑住的是空窗期，服务端一回来就自己回到以服务端为准。运行该场景所用的 `-snapshot` 与 `-hold` 参数见 [greeter 示例](../example/greeter/README.md)。
