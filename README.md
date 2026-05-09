# mGFS - Mini Google File System

A simplified implementation of the Google File System using Python and thriftpy2.

基于 Python 和 thriftpy2 的简化版 Google 文件系统实现。

## Architecture / 架构

```
mGFS/
├── config/
│   ├── gfs.thrift          # Thrift service definitions / Thrift 服务定义
│   ├── gfs-site.xml        # Cluster configuration (Hadoop-style) / 集群配置
│   └── chunkservers        # ChunkServer node list / 节点列表
├── server/
│   ├── master.py           # Master server / 主节点
│   ├── chunkserver.py      # ChunkServer / 数据节点
│   ├── metaData.py         # Metadata classes / 元数据类
│   ├── gfsAPI.py           # Thrift RPC handlers / RPC 处理层
│   └── config_parser.py    # XML config parser / 配置解析器
├── client/
│   ├── client.py           # GFS client / 客户端
│   └── stream.py           # DataQueue & ACKQueue / 流式管道
├── run.py                  # Entry point / 启动入口
├── test_cluster.py         # Integration tests / 集成测试
└── TEST_DOC.md             # Test documentation / 测试文档
```

## Features / 核心特性

| Feature | Description |
|---------|-------------|
| Chunk allocation | Master assigns 64MB chunks with UUID handles / Master 分配 64MB chunk |
| Replication | Configurable replication factor (default 3) / 可配置副本数 |
| Lease mechanism | Primary replica election via time-based leases / 基于时间的租约选举 primary |
| Heartbeat | ChunkServers report status to Master periodically / 心跳监控 |
| Write pipeline | Data flows linearly through replica chain / 数据线性流经副本链 |
| Stream recovery | Excludes failed nodes, rebuilds pipeline / 故障恢复，排除失败节点 |
| Snapshot copy | Copy-on-write chunk replication / 快照复制 |
| Lock safety | Timeout-based locks, no nested locking / 超时锁，无嵌套 |

## Quick Start / 快速启动

### Prerequisites / 环境要求

```bash
pip install thriftpy2
```

### Start Cluster / 启动集群

```bash
# Start all nodes from config (master + chunkservers)
# 从配置文件启动所有节点
python run.py cluster

# Or start individually / 或单独启动
python run.py master --host 127.0.0.1 --port 9090
python run.py chunkserver --id cs1 --host 127.0.0.1 --port 9091
```

### Run Tests / 运行测试

```bash
python test_cluster.py
```

## Configuration / 配置

Hadoop-style XML configuration (similar to `core-site.xml` / `hdfs-site.xml`).

采用 Hadoop 风格的 XML 配置文件。

### gfs-site.xml

| Property | Default | Description |
|----------|---------|-------------|
| `gfs.master.host` | 127.0.0.1 | Master address / 主节点地址 |
| `gfs.master.port` | 9090 | Master port / 主节点端口 |
| `gfs.chunk.size` | 67108864 | Chunk size in bytes (64MB) / 块大小 |
| `gfs.replication` | 3 | Replication factor / 副本数 |
| `gfs.lease.duration` | 60 | Lease timeout in seconds / 租约超时 |
| `gfs.heartbeat.interval` | 5 | Heartbeat interval in seconds / 心跳间隔 |

### chunkservers

```
# Format: host:port  server_id
127.0.0.1:9091  cs1
127.0.0.1:9092  cs2
127.0.0.1:9093  cs3
```

## Client Usage / 客户端使用

```python
from client import GFSClient

client = GFSClient(master_host='127.0.0.1', master_port=9090)
client.register_chunkserver('cs1', '127.0.0.1', 9091)
client.register_chunkserver('cs2', '127.0.0.1', 9092)
client.register_chunkserver('cs3', '127.0.0.1', 9093)

# Create and write / 创建并写入
client.create('/hello.txt')
client.write('/hello.txt', 'hello world')

# Read / 读取
data = client.read('/hello.txt')  # -> 'hello world'

# Append / 追加
client.append('/hello.txt', '!')

# Delete / 删除
client.delete('/hello.txt')
```

## Concurrency & Locking / 并发与锁机制

Each component uses an independent `threading.Lock` with timeout:

每个组件使用独立的超时锁：

- **Master**: protects file/chunk metadata and lease state / 保护元数据和租约状态
- **ChunkServer**: protects chunk data buffer / 保护数据缓冲区
- **DataQueue / ACKQueue**: protects pipeline state / 保护管道状态

Deadlock prevention / 死锁预防:

| Strategy | Implementation |
|----------|---------------|
| Lock timeout | `lock.acquire(timeout=5)` prevents indefinite blocking / 防止无限等待 |
| No nesting | Each method holds only one lock / 每个方法只持有一个锁 |
| RPC isolation | Master and ChunkServer communicate via RPC / 通过 RPC 通信 |
| Lease expiry | Time-based distributed coordination / 基于时间的分布式协调 |

## Testing / 测试

30 test cases covering 7 categories (see `TEST_DOC.md` for details):

30 个测试用例覆盖 7 个类别（详见 `TEST_DOC.md`）：

1. Cluster startup / 集群启动
2. File operations / 文件操作
3. Chunk allocation / Chunk 分配
4. Data read/write / 数据读写
5. Snapshot copy / 快照复制
6. Client end-to-end / 客户端端到端
7. Concurrency / 并发安全

```
========================================
Results: 30 passed, 0 failed, 30 total
========================================
```
