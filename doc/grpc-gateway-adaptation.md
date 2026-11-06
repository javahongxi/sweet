# grpc-gateway 功能调研与 Sweet 的借鉴决策

## 1. 调研结论

grpc-gateway 的核心不是 Envoy 式通用代理，而是“`protoc` 插件 + HTTP/gRPC transcoding runtime”。
它从 `google.api.http` 或外部 gRPC API Configuration 读取映射，生成静态反向代理代码，再为静态 API
生成 OpenAPI。Sweet 则把 Nacos 寻址、配置路由、原子热更新与缓存容灾放在运行时网关中。两者可以共享
HTTP 转码语义，但不应共享同一套架构。

本次借鉴的是**转码协议和 descriptor 使用方式**，没有把 `protoc-gen-grpc-gateway`、生成代理或
grpc-gateway runtime 引进 Sweet，也没有增加第三方依赖。

调研依据是本地 grpc-gateway 仓库的 README、生成器入口、`runtime` 的 mux/query/context/handler/errors、
生成示例以及 mapping/operations/OpenAPI 文档，而不是只读项目宣传。调研过程没有修改该仓库。

## 2. grpc-gateway 已实现的主要能力

| 方向 | 能力 |
| --- | --- |
| 映射来源 | `google.api.http` 注解、外部 YAML、`generate_unbound_methods`、standalone 模式 |
| 接入方式 | 后端 endpoint、已有 `*grpc.ClientConn`、进程内 service |
| HTTP 绑定 | path/query/body、`body:"*"`、字段 body、`response_body`、`additional_bindings` |
| 字段类型 | 嵌套字段、repeated、map、enum、bytes 与常见 Well-Known Types |
| 路径语法 | 单段与多段通配、多种 unescaping mode、PATCH 自动生成 FieldMask |
| 序列化 | `protojson`/`JSONPb`、按 MIME 注册 Marshaler、自定义流分隔符、`google.api.HttpBody` |
| 调用形态 | unary 与 server-streaming；server stream 转 NDJSON；不提供真正的 HTTP client/bidi streaming |
| metadata | 请求 Header 到 gRPC metadata、响应 Header/Trailer、binary metadata、X-Forwarded-For/Host |
| 超时 | `Grpc-Timeout`，支持 H/M/S/m/u/n 单位 |
| 错误 | gRPC code 到 HTTP status；unary、stream、routing 三类错误处理器 |
| 扩展 | middleware、response rewriter、自定义路由、metadata annotator、健康检查、OpenTelemetry |
| 文档 | 稳定的 OpenAPI 2.0 生成器；所调研 main 分支的 OpenAPI 3.1 仍标记 Alpha；另有文档合并工具 |

它还有明确的非目标：不计划支持 XML、真正的 HTTP 双向流，以及把 RPC 方法参数直接绑定到 HTTP Header。
multipart 文件上传也不是自动 transcoding 能力，需要自定义 handler。

## 3. Sweet 原有优势与差距

Sweet 原有的差异化能力不在 grpc-gateway 的职责中心：

- `registry/direct` 与手写 Nacos gRPC 协议注册发现；
- `RouteSpec` 配置化路由，查找名与线上 gRPC service 名可以不同；
- 整表预校验、失败保留旧表、成功原子换表；
- 按查找名复用 `grpc.ClientConn`；
- 最后一次成功路由表落盘，配置中心不可达时按 source/timestamp 校验后恢复；
- 默认严格 JSON、1 MiB 请求上限、请求 Header 白名单、5xx 后端文本屏蔽。

原实现的不足是：只支持精确路径和整体 JSON body；Nacos 模式要手填 `Options.Services` 消息工厂；
不能暴露响应 metadata；流式成功帧直接输出响应消息，响应自己的顶层 `error` 字段会与终错帧冲突。
gRPC code 到 HTTP status 的映射则已经与 grpc-gateway 基本一致，不需要重造。

## 4. 本次实现的协议

### 4.1 Path/query/body 与 responseBody

`RouteSpec` 新增两个字段：

| 字段 | 含义 |
| --- | --- |
| `body` 缺省 | 兼容旧配置，按整个请求消息解码 JSON body |
| `body:"*"` | 显式绑定整个请求消息，query 不再重复填充 |
| `body:""` | 不解码 body，只使用 path/query |
| `body:"field"` / `"a.b"` | 把 body 解码到指定字段，支持标量、message、list、map |
| `responseBody` 缺省 | 返回完整响应消息 |
| `responseBody:"field"` / `"a.b"` | 只返回选定字段，unary 与每个流帧均生效 |

请求组装顺序固定为 body → path → query。path 覆盖同名字段；query 只填充未被 path/body 占用的字段。
query 支持 proto/JSON 字段名、嵌套字段、repeated、map、enum、bytes，以及 Timestamp、Duration、
FieldMask、wrapper、Struct/Value。未知 body/query 字段默认返回 400，`AllowUnknownFields` 开启后忽略。
同一字段同时使用 proto 名和 JSON 名会被拒绝，而不是依赖 map 遍历顺序决定谁赢。

路径模板当前只支持 `{field}` 和 `{a.b}` 占据一个完整路径段。精确路径优先，其后按静态段数量和配置
顺序匹配；仅变量名不同的等价模板会被判重。`*`、`**`、自定义 verb、PATCH FieldMask 不在当前范围。
响应字段提取保留 protojson 的 proto 字段名和 int64 字符串编码，不经过 `map[string]any` 的 float64 转换。

示例：

```json
{"method":"GET","path":"/v1/greeter/{name}","service":"greeter-provider",
 "fullMethod":"/sweet.example.v1.Greeter/GreetStream","body":"","responseBody":"message"}
```

### 4.2 已编译 descriptor 自动解析

`ApplyRoutes` 按以下顺序解析方法：

1. 如果 `Options.Services` 显式配置了该 gRPC service，沿用其中的构造器，保持定制与兼容；
2. 否则按 `FullMethod` 在 `protoregistry.GlobalFiles` 查 service/method descriptor；
3. 优先通过 `protoregistry.GlobalTypes` 创建生成的 Go 消息，缺少生成类型时用 `dynamicpb`；
4. 从 method descriptor 自动得到 server-streaming 属性，旧配置中的 `serverStream:true` 仍可保留。

这里的“自动注册”指使用**网关进程已经链接的 Proto descriptor**，不是调用 provider 的 reflection 服务，
也不是读取 FileDescriptorSet。后端可以关闭 reflection；网关的 Proto Go 包则必须被 import 链接进二进制。
client/bidi streaming、未知方法和所有绑定字段错误都在换表前拒绝，不留下半张新表。

`Handle` 的显式构造器 API 不变。descriptor 文件加载是另一个配置与生命周期问题，本次没有顺手加入。

### 4.3 响应 metadata

新增两个默认空白的白名单：

- `Options.ForwardResponseHeaders`：允许返回给 HTTP 客户端的 gRPC Header；
- `Options.ForwardResponseTrailers`：允许暴露的 gRPC Trailer。

业务 Header 使用原 Header 名返回。Trailer 只在请求声明 `TE: trailers` 时，以 `Grpc-Trailer-*` 暴露；
声明发生在 `WriteHeader` 之前，实际值在 RPC 或流结束后写入。`-bin` 值按 base64 输出，非法 metadata key、
hop-by-hop/framing 字段和 `grpc-*` 保留字段不会因写进白名单而放行。

流式响应在提交 200 前取得 Header，在 EOF/错误后写 Trailer。默认仍不转发 Authorization，也不因支持
响应 metadata 而放宽 5xx 错误文本屏蔽。

### 4.4 流式 result/error 信封

当前 NDJSON 协议为：

```json
{"result":{"message":"Hello #1, web!"}}
{"result":{"message":"Hello #2, web!"}}
```

提交 200 后发生终错：

```json
{"error":{"code":"PERMISSION_DENIED","codeNumber":7,"message":"backend message"}}
```

`responseBody:"message"` 时成功值直接是字符串：

```json
{"result":"Hello #1, web!"}
```

首帧前错误仍使用普通 HTTP 状态和普通错误体；零帧仍是 200 加空 body；带内终错不含 `httpStatus`。
这是已确认的**破坏性协议调整**：旧版成功帧是裸响应对象，现有 NDJSON 客户端必须同步读取 `result`。
统一信封换来了成功/失败的无歧义判别，响应消息自己的顶层 `error` 字段不再构成冲突。

## 5. 明确保留与明确不做

保留：Nacos 原子换表、缓存格式与恢复、连接复用、严格 JSON 默认值、请求体上限、错误屏蔽、显式
`Options.Services`、`Handle` 构造器、 unary 响应 proto 字段名和零值输出。

本次不做：

- protoc 代码生成架构或 grpc-gateway runtime 依赖；
- descriptor 文件、服务端 reflection 动态发现；
- `*`/`**` 路径、method override、form fallback、XML、HTTP client/bidi streaming；
- 默认转发 Authorization、默认忽略未知字段；
- 通用 Marshaler 注册、可插拔错误详情、`Grpc-Timeout`；
- OpenAPI、独立 HTTP 健康检查适配和 OpenTelemetry 接入。

静态 API 如果确实需要 grpc-gateway 的完整注解能力，可以把 Sweet `client.Dial` 返回的标准
`*grpc.ClientConn` 交给其生成的 `RegisterXHandler`。那是一种显式组合方案，不是 Sweet 动态网关的主路线。

## 6. 后续建议顺序

1. 评估是否增加 `Grpc-Timeout`，与现有 HTTP context deadline、`RequestTimeout` 明确取最短期限；
2. 在保持 5xx 屏蔽的前提下评估 `google.rpc.Status.details` 和自定义错误处理器；
3. 从同一规范化路由表与 descriptor 生成 OpenAPI，避免 Proto、Nacos 配置和文档三处漂移；
4. 增加标准 gRPC health check 的 HTTP 适配和 OpenTelemetry 示例；
5. 只有出现真实配置需求时，再考虑 `*`/`**` 路径与 descriptor 文件，不提前扩大协议面。

## 7. 验证设计

网关测试使用真实 sweet server、direct registry 和生成的 Greeter stub，验证自动构造的消息能与标准
生成服务端互操作；复杂字段通过测试内动态 descriptor 覆盖，不修改生成的 pb。响应 Trailer 使用真实
HTTP server 验证，避免只看 `ResponseRecorder` 的伪语义。坏配置同时断言线上表与缓存不被替换。

验证命令：

```sh
go test ./gateway
go test ./...
```

这些均为离线测试，不连接外部 Nacos，也不把历史真机日志当成本次升级的实测结果。
