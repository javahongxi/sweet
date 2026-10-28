# 跨语言：sweet 调 jaws 的 wire provider

对端是 **jaws 的 Java gRPC 服务端**（Spring Boot 那个样例，`WireBootProvider`）：服务实现、
接口和 proto 都是 jaws 自己的，sweet 这边一行都不必知道。调用侧是 `greeter.proto` 生成的标准
Go 桩，只是 `conn` 由 `client.Dial` 给出。所以这一跑证明的是两件不同的事：wire 上真互通，
以及 sweet 的拨号链路不破坏 gRPC 语义。

两条寻址方式都通，差别只在地址怎么来：`-registry=direct` 从命令行拿 host:port（只测线格式
互通，别掺进发现），`-registry=nacos` 由 Nacos 给出地址（跨语言 + 外部注册中心那一格）。

## 起对端

jaws 侧需要 Nacos 在 `127.0.0.1:8848`，然后从 jaws 仓库根目录：

```
mvn -pl jaws-samples/jaws-sample-wire-boot-provider spring-boot:run
```

监听 50051，同时提供 `greeter.Greeter` 与 `calculator.Calculator`，并把两个服务注册进 Nacos：
`default_rpc` 组下的 `jaws/org.hongxi.jaws.sample.wire.proto.GreeterService`，实例 metadata 带
`protocol=wire`、`path=<接口全名>` 等 jaws 自己的哨兵键。为了走注册中心，jaws 侧只改了导出配置
（`application.yml` 的 registry 地址 + 一个 `jaws-registry-nacos` 依赖），`GreeterService` 与它的
proto 一行未动。

## 跑客户端

直连（默认）：

```
go run ./example/jaws-wire -name sweet
```

经 Nacos 发现——注意查找名与 gRPC 服务名是两串不同的字符串，`-resolve` 给前者：

```
go run ./example/jaws-wire -registry=nacos -group default_rpc -resolve jaws/org.hongxi.jaws.sample.wire.proto.GreeterService -name sweet
```

实测输出（2026-10-28，两次都 rc=0；nacos 那发多一行解析记录，其余逐字相同）：

```
resolved jaws/org.hongxi.jaws.sample.wire.proto.GreeterService in group default_rpc from nacos: [{10.62.19.145 50051 1 map[application:sample-wire-boot-provider compression:gzip endpointType:service path:org.hongxi.jaws.sample.wire.proto.GreeterService protocol:wire serialization:fastjson2 timestamp:1793160935967]}]
calling /greeter.Greeter/SayHello against nacos found as "jaws/org.hongxi.jaws.sample.wire.proto.GreeterService"
unary  SayHello        → "Hello, sweet! (from jaws-wire-boot)"
stream SayHelloStream → "Hello #1, sweet! (from jaws-wire-boot stream)"
stream SayHelloStream → "Hello #2, sweet! (from jaws-wire-boot stream)"
stream SayHelloStream → "Hello #3, sweet! (from jaws-wire-boot stream)"
```

第一行是这条链的关键证据：地址、组、以及 jaws 自己写的元数据都来自注册中心，不是命令行给的。
它能被忽略也没关系——`toInstances` 只取 host/port/weight，metadata 原样带过来不做判断。

## 顺手记一个互通坑

对端换成 boot provider 之后，这个例子一度稳定失败：unary 和三帧流式都拿到，然后连接被
`GOAWAY too_many_pings` 掐掉。根因在 jaws 服务端一处 keepalive 执法：`WireKeepaliveHandler`
只在**入站**帧上重置"有无数据"的判断窗口，而服务端流式响应期间入站本来就是静的，于是 grpc-go
客户端那段时间里的 PING 被连记两次 strike 就杀连接（`grpcurl` 打同一个流方法也复现）。已在
jaws 侧修掉（出站帧同样算数据，`outboundFramesCountAsDataToo` 钉住，空闲连接的连续 PING 仍然会
被抓），jaws-wire 206 条用例全过。

## 三处刻意保持不变

- **gRPC 服务名只有一套**：`greeter.Greeter` 直接来自 proto 的 `package` + `service`，与
  jaws 服务端声明的完全一致，`-resolve` 传的那个 `jaws/<接口全名>` 只是 jaws 在注册中心里的
  查找键，不是第二套 RPC 标识——出站 `:path` 里出现的始终是前者。两串能同时成立，靠的是 jaws
  服务端把 Java 接口名和 proto 全名都登记了一次。
- **调用点不为一件事改过一行**：`greeterpb.NewGreeterClient(conn)` 是生成码原样，换寻址方式只
  影响 `conn` 从哪来。
- **proto 副本只加了 `go_package`**：`greeter.proto` 是从 jaws 的
  `jaws-sample-wire-api/src/main/proto/greeter.proto` 复制的，package/service/method
  字段一字未改，`java_*` 选项也留着（对 Go 生成无影响）。加 `go_package` 只影响生成代码的
  导入路径，不碰线上格式。

重新生成桩不需要手敲 protoc —— 配方写成了 `go:generate`，就地可跑（完整命令见
`greeterpb/doc.go`）：

```
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go generate ./example/jaws-wire/greeterpb
```

这些插件是**代码生成期的工具**，不引入任何运行时依赖 —— 产物只 import
`google.golang.org/grpc` 与 `google.golang.org/protobuf`，都在现有依赖里。
（`gen/` 那边故意不用 grpc 桩，只生成 message 类型，方法名写在代码里。）

上面第三条已做过破坏性验证：删掉两份产物后重跑，重建结果与原件逐字节一致。
第一次跑失败过，原因是 pragma 里把插件写成裸名 `protoc-gen-go`，而 protoc 按**它自己的
PATH** 找可执行文件，构建/CI 环境常常没有 `~/go/bin` —— 现在写的是 `$HOME/go/bin/...`
绝对路径（包在 `sh -c` 里才能展开 `$HOME`，`go:generate` 自身不做变量展开）。
