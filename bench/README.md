# bench —— sweet 的吞吐基准

## 这个基准能证明什么，不能证明什么

能证明：sweet 相对裸 grpc-go 的**每调用开销**（同一台机器、同一形状、交错跑）。

不能证明：协议优劣。和 jaws 的表并排时，一次比较同时换了语言运行时、网络库、协议实现与序列化，
差值只能记成**栈 vs 栈**。谁把它说成"gRPC 比 wire 快/慢"，就是没控制变量。

口径与 jaws 的 `doc/benchmark.md` 对齐：闭环、固定并发、预热丢弃、延迟合并取分位、
provider 与 consumer 分进程（对应 jaws 的 `ROLE=provider` / `ROLE=consumer`）、loopback TCP。
payload 同为 `HelloRequest{name="benchmark"}`，回复文本等长。并发 20、预热 5s、测量 15s 或 40s，
与 jaws 表里 `THREADS=20 WARMUP=5 DURATION=40` 同形。

## 跑法

```
# 终端 1
go run ./bench -mode provider -server sweet -port 50051

# 终端 2
go run ./bench -mode consumer -client sweet -addr 127.0.0.1:50051 -threads 20 -warmup 5 -duration 40
```

`-server` / `-client` 各取 `native`（裸 grpc-go）或 `sweet`，四组组合都在下面。跨语言那格把
provider 换成 jaws 的：

```
cd /path/to/jaws && ROLE=provider SERVER=wire PORT=50051 ./run-sample.sh bench-grpc
```

## 环境

Apple M2（8 核）、go1.27.3 darwin/arm64、grpc-go v1.84.0、loopback、单条客户端连接上 20 个并发
调用。测量日期 2026-10-29。jaws 侧数字取自其 `doc/benchmark.md`（2026-09-25 记录，同为 macOS
aarch64 8 核），**未重跑**。

## 先说方法学结论：不交错就是假的

同一台机器持续负载下会漂。首回合吃到冷启动红利，之后落到稳定值：

| 轮次 | native↔native | sweet↔sweet |
| --- | --- | --- |
| 1 | 102,146 | 89,021 |
| 2 | 85,596 | 82,584 |
| 3 | 87,290 | 87,632 |
| 4 | 87,763 | 88,606 |

单次 40s 各跑一遍会得出 `105,502 vs 105,793`，看着"完全持平"；不交错连跑三次则 native 一路
105,496 → 101,066 → 99,715、sweet 一路 89,957 → 83,925 → 83,864，会误判成"sweet 慢 15%"。
只有 A/B/A/B 交错之后差异才塌缩：**稳定态两臂都在 83k–89k 区间重叠，sweet 的每调用开销落在
噪声里**——符合预期，`Register`/`Dial` 只作用于建连与订阅，不在每调用路径上。

结论：**任何单次数字都不要引用**；要引用就引交错后的中位数，并写明时长。

## 并发度扫描：sweet 的峰值与"20 线程"这个前提

jaws 表里的 142,876 与 20 线程、分进程、DURATION=40s 绑定；它那条"25 线程降到 98,785"取自
**同进程**配置，不是分进程长驻那套。所以"20 线程就是极限"在分进程口径下并未被验证过。Go 侧扫下来
曲线根本没在 20 见顶：

| threads | native↔native | sweet↔sweet |
| --- | --- | --- |
| 20 | 103,808 / 104,076 | 103,962 / 100,661 |
| 40 | 126,089 / 126,608 | 125,238 / 128,193 |
| 80 | 142,105 / 141,592 | 138,384 / 139,742 |
| 160 | 149,096 / 151,305 | 148,867 / 150,145 |
| 320 | ≈150k（峰值附近） | ≈150k |
| 640 | 152,863（P99 5,950us） | 151,896（P99 6,052us） |

320 那一档第一遍看到 `159,050 vs 153,785`（native 高 3.3%），像"sweet 有开销"。按同一规则重复到
**各 6 个样本**后：native 中位 ≈150.5k、sweet 中位 ≈150.4k，两臂各自散布 ±8% —— 差值塌回噪声。
这就是为什么单次数字不能引：这台机器的噪声水平是 8%，任何小于它的"结论"都是自我说服。

结论三条：
1. **sweet 的每调用开销在本机分辨率下测不出来**（≤8%），跨 20→320 全档一致。
2. Go 侧峰值约 **150k–159k**，出现在 320 线程附近；640 不再涨、延迟翻倍，是天花板而非甜点。
3. 拿 150k 去比 jaws 的 142,876 **不成立**：一个是我方峰值、一个是对方 20 线程点。要真正比峰值，
   得把 Java 侧也扫一遍（`bench-wire`/`bench-grpc` 用 `-Dthreads=320` 分进程），否则只能比
   同一点位：20 线程下 Java jaws-TCP 142,876 vs Go sweet 栈约 104k（不同栈，见开头那条警告）。

## 跨语言那一格

Go 客户端 → jaws 的 wire benchmark provider（静默实现），交错两轮：

| 轮次 | native client | sweet client |
| --- | --- | --- |
| 1 | 95,147 | 96,942 |
| 2 | 97,866 | 97,478 |

全部 0 错误。对照 jaws 表里同一服务端的 `grpc-java → wire = 104,967`：**同一 Java 服务端，
Go 客户端与 Java 客户端在同一量级**，差值在跨天、跨机况的范围内，不构成结论。

一个必须记的反例：把 provider 换成 `jaws-sample-wire-boot-provider`，同一客户端只跑到
**约 39.3k–39.8k**。原因是它的 `sayHello` 每次调用都 `log.info` 一行（
`GreeterServiceImpl.java:35`）。这不是协议差异，是被测服务端自己加了 IO——跨语言比吞吐时
必须确认两端 handler 都是静默的。

## 与 jaws 整表并排（栈 vs 栈）

| 组合 | QPS | 来源 |
| --- | --- | --- |
| grpc-go ↔ grpc-go（sweet 路径） | 稳定态 83k–89k | 本文，2026-10-29，交错 |
| grpc-go → jaws wire | 95k–98k | 本文，2026-10-29，交错 |
| grpc-java → grpc-java | 70,334 | jaws `doc/benchmark.md`，2026-09-25 |
| grpc-java → jaws wire | 104,967 | 同上 |
| jaws wire → jaws wire | 89,945 / 92,423(direct) | 同上 |
| jaws TCP → jaws TCP | 峰值 142,876 / 均值 140,692 | 同上 |

读法：Go↔Go 与 Java↔Java 不在同一天、不共享校准，只能看量级不能作差；唯一在同一进程外可控的
对比是"同一 Java 服务端、Go 客户端 vs Java 客户端"那一行。
