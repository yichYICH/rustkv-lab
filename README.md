# RustKV-Lab

RustKV-Lab 是一个使用 Rust 实现的轻量级异步键值数据库系统。项目目标是展示 Rust 在网络服务、协议解析、并发共享状态、持久化和工程化测试中的综合应用。它借鉴 Redis-like 数据库的基本思想，但不是完整 Redis 替代品，也不面向生产环境直接部署。

本项目适合作为轻量级数据库原型：既能通过 CLI 和 TUI 展示可运行效果，也能通过文档、单元测试、集成测试和 benchmark 工具说明系统设计与工程质量。

## 1. 项目定位

RustKV-Lab 的定位是“教学型异步键值数据库实验系统”。

它重点覆盖：

- RESP 协议子集的解析与编码。
- Tokio 异步 TCP server。
- 基于分片 `HashMap<String, Entry>` 的内存键值存储。
- Redis-like 基础命令执行。
- TTL 过期模型。
- AOF 命令日志持久化。
- 服务端运行参数集中配置。
- CLI 客户端、TUI 监控和 benchmark 工具。
- 单元测试与真实 TCP 集成测试。

它暂不覆盖：

- Redis 完整命令集。
- 集群、主从复制和哨兵。
- RDB 快照。
- AOF rewrite。
- 鉴权、TLS 和公网安全部署。
- 精确内存管理与淘汰策略。

## 2. 功能清单

| 功能           | 说明                                                                                   |
| -------------- | -------------------------------------------------------------------------------------- |
| TCP server     | 基于 Tokio 监听 TCP 连接，每个客户端连接由独立异步任务处理                             |
| RESP 协议解析  | 支持 Simple String、Error、Integer、Bulk String、Array、Null                           |
| 命令系统       | 支持 `PING`、`SET`、`GET`、`DEL`、`EXISTS`、`KEYS`、`EXPIRE`、`TTL`、`FLUSHDB`、`INFO` |
| 分片存储       | `ShardedDatabase` 按 key 自动路由到 shard，客户端命令不需要指定 shard                 |
| 服务端配置     | `ServerConfig` 集中管理监听地址、AOF 路径、frame 限制、TTL 扫描间隔和 shard 数量       |
| TTL 过期       | 支持惰性删除和后台 worker 主动清理                                                     |
| AOF 持久化     | 写命令以 RESP frame 形式追加到 AOF 文件，重启时回放恢复                                |
| CLI 客户端     | 支持一次性命令和长连接交互式 TUI shell                                                 |
| TUI 监控       | 使用 Ratatui 展示连接数、key 数量、命令计数、QPS、AOF 状态等                           |
| benchmark 工具 | `rustkv-bench` 输出请求数、耗时、平均延迟和 QPS                                        |
| 单元测试       | 覆盖协议、命令解析、数据库核心逻辑                                                     |
| 集成测试       | 使用动态端口启动真实 server，测试 TCP 命令、TTL、AOF reload 和 frame 限制              |

## 3. Workspace 项目结构

```text
rustkv-lab
├── Cargo.toml
├── README.md
└── crates
    ├── rustkv-protocol
    ├── rustkv-core
    ├── rustkv-server
    ├── rustkv-cli
    ├── rustkv-monitor
    └── rustkv-bench
```

| Crate             | 职责                                                  |
| ----------------- | ----------------------------------------------------- |
| `rustkv-protocol` | RESP 协议 AST、零拷贝 parser、owned encoder           |
| `rustkv-core`     | 数据库核心、存储抽象、命令状态机、执行器、统计信息    |
| `rustkv-server`   | Tokio TCP 服务端、连接缓冲、AOF、TTL worker、优雅关闭 |
| `rustkv-cli`      | 命令行客户端，支持一次性请求和交互式长连接 shell      |
| `rustkv-monitor`  | TUI 可视化监控台，通过 `INFO` 轮询 server             |
| `rustkv-bench`    | 简单 benchmark / 压测工具                             |

依赖方向保持单向：

```text
protocol <- core <- server
protocol <- cli
protocol <- monitor
protocol <- bench
```

## 4. 编译与测试

进入项目目录：

```powershell
cd path\to\rustkv-lab
```

检查 workspace 编译：

```powershell
cargo check --workspace
```

运行全部测试：

```powershell
cargo test --workspace
```

单独运行关键 crate 测试：

```powershell
cargo test -p rustkv-protocol
cargo test -p rustkv-core
cargo test -p rustkv-server
```

服务端集成测试位于：

```text
crates/rustkv-server/tests/integration_tests.rs
```

集成测试使用 `127.0.0.1:0` 动态端口，不占用固定 `6379`。

## 5. 启动服务端

普通启动，使用默认配置 `127.0.0.1:6379`：

```powershell
cargo run -p rustkv-server
```

指定监听地址：

```powershell
cargo run -p rustkv-server -- --addr 127.0.0.1:6379
```

启用 AOF 持久化：

```powershell
cargo run -p rustkv-server -- --addr 127.0.0.1:6379 --aof rustkv.aof
```

说明：

- `--addr` 指定监听地址。
- `--aof` 指定 AOF 日志文件路径。
- `--max-frame-size` 指定单个 RESP frame 的最大字节数，默认 `1048576`。
- `--ttl-interval-ms` 指定后台 TTL worker 的扫描间隔，默认 `1000` 毫秒。
- `--shards` 指定内存数据库 shard 数量，默认 `16`。客户端仍然只需要使用普通 `SET` / `GET` / `DEL` 命令，server 会根据 key 自动选择 shard。
- 按 `Ctrl+C` 可触发关闭：停止 accept 新连接、通知 TTL worker 退出、等待连接任务收尾并刷新 AOF。

示例：调整 frame 限制、TTL 扫描间隔和 shard 数量：

```powershell
cargo run -p rustkv-server -- --max-frame-size 2097152 --ttl-interval-ms 500 --shards 32
```

## 6. CLI 使用示例

写入数据：

```powershell
cargo run -p rustkv-cli -- --addr 127.0.0.1:6379 set name rust
```

读取数据：

```powershell
cargo run -p rustkv-cli -- --addr 127.0.0.1:6379 get name
```

删除数据：

```powershell
cargo run -p rustkv-cli -- --addr 127.0.0.1:6379 del name
```

判断 key 是否存在：

```powershell
cargo run -p rustkv-cli -- --addr 127.0.0.1:6379 exists name
```

查看所有 key：

```powershell
cargo run -p rustkv-cli -- --addr 127.0.0.1:6379 keys
```

设置 TTL：

```powershell
cargo run -p rustkv-cli -- --addr 127.0.0.1:6379 set token abc --ex 5
cargo run -p rustkv-cli -- --addr 127.0.0.1:6379 ttl token
```

清空数据库：

```powershell
cargo run -p rustkv-cli -- --addr 127.0.0.1:6379 flushdb
```

查看服务端状态：

```powershell
cargo run -p rustkv-cli -- --addr 127.0.0.1:6379 info
```

`INFO` 返回 JSON BulkString，示例：

```json
{
  "server_version": "0.1.0",
  "role": "standalone",
  "uptime_seconds": 60,
  "memory_estimate_bytes": 256,
  "aof_enabled": true,
  "addr": "127.0.0.1:6379",
  "max_frame_size": 1048576,
  "total_commands": 10,
  "connected_clients": 1,
  "key_count": 2,
  "expired_keys": 1,
  "get_count": 3,
  "set_count": 4,
  "del_count": 1
}
```

启动交互式长连接 TUI 客户端：

```powershell
cargo run -p rustkv-cli -- --addr 127.0.0.1:6379 shell
```

进入 shell 后可以直接输入：

```text
ping
set name rust
get name
expire name 30
ttl name
info
quit
```

该模式只建立一次 TCP 连接，之后在同一个连接里连续发送多条 RESP 命令。它更接近真实数据库客户端的交互方式，也能直观展示“server 常驻、client 长连接访问”的系统结构。

## 7. 支持命令

| 命令                       | 说明                     |
| -------------------------- | ------------------------ |
| `PING`                     | 心跳检测，返回 `PONG`    |
| `SET key value`            | 写入 key/value           |
| `SET key value EX seconds` | 写入并设置秒级 TTL       |
| `SET key value PX millis`  | 写入并设置毫秒级 TTL     |
| `GET key`                  | 读取 key                 |
| `DEL key [key ...]`        | 删除一个或多个 key       |
| `EXISTS key`               | 判断 key 是否存在        |
| `KEYS`                     | 返回当前 key 列表        |
| `EXPIRE key seconds`       | 给已有 key 设置 TTL      |
| `TTL key`                  | 查看剩余 TTL             |
| `FLUSHDB`                  | 清空数据库               |
| `INFO`                     | 返回 JSON 格式服务端状态 |

TTL 返回值说明：

| 返回值 | 含义                         |
| ------ | ---------------------------- |
| `> 0`  | key 存在，并且设置了过期时间 |
| `-1`   | key 存在，但没有过期时间     |
| `-2`   | key 不存在或已经过期         |

## 8. TUI 监控

启动监控台：

```powershell
cargo run -p rustkv-monitor -- --addr 127.0.0.1:6379
```

监控台每秒向 server 发送一次 `INFO`，展示：

- 服务端地址、版本、角色。
- AOF 是否启用。
- uptime。
- connected clients。
- key count。
- expired keys。
- total commands。
- GET / SET / DEL count。
- QPS。
- 最大 frame 限制。

按 `q` 退出监控台。

## 9. Benchmark 压测工具

`rustkv-bench` 用于展示基础吞吐量。它通过真实 TCP 连接访问 server，并使用 `rustkv-protocol` 编码 RESP 请求，因此测试结果覆盖客户端编码、网络传输、服务端解析、命令执行和响应返回。

SET 压测：

```powershell
cargo run -p rustkv-bench -- --addr 127.0.0.1:6379 --requests 1000 --command set
```

GET 压测：

```powershell
cargo run -p rustkv-bench -- --addr 127.0.0.1:6379 --requests 1000 --command get
```

混合压测：

```powershell
cargo run -p rustkv-bench -- --addr 127.0.0.1:6379 --requests 1000 --command mixed
```

多连接压测：

```powershell
cargo run -p rustkv-bench -- --addr 127.0.0.1:6379 --requests 10000 --command mixed --clients 4
```

输出字段：

| 字段               | 含义           |
| ------------------ | -------------- |
| `total_requests`   | 完成请求数     |
| `total_elapsed_ms` | 总耗时         |
| `avg_latency_ms`   | 平均单请求延迟 |
| `qps`              | 每秒请求数     |

报告表格模板：

| 测试场景     | requests | clients | total_elapsed_ms | avg_latency_ms | qps | 备注            |
| ------------ | -------: | ------: | ---------------: | -------------: | --: | --------------- |
| SET 单连接   |     1000 |       1 |                  |                |     | AOF 关闭        |
| GET 单连接   |     1000 |       1 |                  |                |     | 预热 key 后计时 |
| MIXED 单连接 |     1000 |       1 |                  |                |     | SET/GET 交替    |
| MIXED 多连接 |    10000 |       4 |                  |                |     | 多 TCP 连接并发 |

## 10. Rust 核心特性映射

| Rust 特性                 | 项目映射                                                                |
| ------------------------- | ----------------------------------------------------------------------- |
| ownership / borrowing     | `Database` shard 拥有 `String` 和 `Vec<u8>`；parser 借用输入 buffer     |
| lifetime                  | `RespFrame<'a>` 将解析结果生命周期绑定到输入字节流                      |
| struct / enum             | `Entry`、`Database`、`ShardedDatabase`、`ServerConfig`、`ServerStats`、`Command`、`RespFrame`、`RespValue` |
| trait                     | `StorageEngine` 抽象数据库存储行为                                      |
| 泛型                      | `to_json_string<T: serde::Serialize>`                                   |
| `Result` 错误处理         | `ProtocolError`、`KvError`、I/O 错误逐层返回                            |
| `Arc` / `RwLock`          | 多连接异步任务共享分片数据库，每个 shard 独立加锁                       |
| atomic                    | `ServerStats` 使用原子计数记录命令数、连接数、key 数量等指标            |
| `async` / `await`         | TCP accept、read/write、AOF 文件 I/O、TTL worker                        |
| cargo workspace           | 多 crate 分层构建和统一测试                                             |
| cargo fmt / clippy / test | 格式化、静态检查和自动化测试保证工程质量                                |

## 11. Windows 编译说明

正常情况下不需要手动配置 linker：

```powershell
cargo check --workspace
```

如果 Windows 环境提示 `linker not found`、`dlltool` 或链接错误，先确认安装目标：

```powershell
rustup target add x86_64-pc-windows-msvc
```

本项目的 `.cargo/config.toml` 默认使用标准 Windows MSVC target：

```toml
[build]
target = "x86_64-pc-windows-msvc"
target-dir = "C:/Temp/rustkv-lab-target"
```

这里没有配置任何本机 LLVM / MSVC linker 绝对路径。如果仍然需要显式指定 linker，可以参考 `.cargo/config.example.toml`，但必须替换为本机真实路径。不同机器路径不同，不能直接使用他人电脑上的 LLVM / MSVC 绝对路径。

如果 `cargo test --workspace` 在 Windows GNU 工具链下出现 `dlltool` / `Invalid bfd target` 之类的环境错误，可以使用 MSVC target 验证：

```powershell
cargo test --workspace --target x86_64-pc-windows-msvc --target-dir C:\Temp\rustkv-lab-target-msvc
```

该命令不改变源码，只是把测试构建切换到 MSVC target 和一个 ASCII-only 的临时构建目录。
