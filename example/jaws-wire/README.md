# 跨语言：sweet 调 jaws 的 wire provider

对端是 **jaws 的 Java gRPC 服务端（`WireProvider`）**，我们改不了它一行代码；调用侧是
`greeter.proto` 生成的标准 Go 桩，只是 `conn` 由 `client.Dial` 给出。所以这一跑证明的是
两件不同的事：wire 上真互通，以及 sweet 的拨号链路不破坏 gRPC 语义。

## 起对端

jaws 的 `./run-sample.sh wire` 是 provider+consumer 一次性演示（跑完自己停），
要一个常驻 provider 得单独起 `WireProvider`。从仓库根目录：

```
cd /Users/hongxi/dev/jaws
DEPS=$(mvn -pl jaws-samples/jaws-sample-wire-provider dependency:build-classpath \
  -DincludeScope=runtime -Dmdep.outputFile=/dev/stdout -DexcludeGroupIds=org.hongxi -q | tail -1)
java -cp "jaws-stream-api/target/classes:jaws-core/target/classes:\
jaws-samples/jaws-sample-api/target/classes:jaws-wire-proto/target/classes:\
jaws-wire/target/classes:jaws-samples/jaws-sample-wire-api/target/classes:\
jaws-samples/jaws-sample-wire-provider/target/classes:$DEPS" \
  org.hongxi.jaws.sample.wire.provider.WireProvider
```

监听 50051，同时提供 `greeter.Greeter` 与 `calculator.Calculator`，不注册任何注册中心
（wire 样例走直连导出）。

## 跑客户端

```
go run ./example/jaws-wire -name sweet
```

实测输出：

```
unary  SayHello        → "Hello, sweet! (from jaws-wire)"
stream SayHelloStream → "Hello #1, sweet! (from jaws-wire stream)"
stream SayHelloStream → "Hello #2, sweet! (from jaws-wire stream)"
stream SayHelloStream → "Hello #3, sweet! (from jaws-wire stream)"
```

## 两处刻意保持不变

- **服务名只有一套**：`greeter.Greeter` 直接来自 proto 的 `package` + `service`，与
  jaws 服务端声明的完全一致；示例里没有任何第二套标识。
- **proto 副本只加了 `go_package`**：`greeter.proto` 是从 jaws 的
  `jaws-sample-wire-api/src/main/proto/greeter.proto` 复制的，package/service/method
  字段一字未改，`java_*` 选项也留着（对 Go 生成无影响）。加 `go_package` 只影响生成代码的
  导入路径，不碰线上格式。

生成桩需要 `protoc-gen-go-grpc`（`go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest`）；
它是**代码生成期的工具**，不引入任何运行时依赖 —— 产物只 import `google.golang.org/grpc` 与
`google.golang.org/protobuf`，都在现有依赖里。（这与 `gen/` 的做法不同：那里故意不用 grpc 桩。）
