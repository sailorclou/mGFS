# mGFS 集群测试文档

## 1. 测试目标

验证 mGFS 分布式文件系统的核心功能在集群环境下正确运行，包括：
- Master 与 ChunkServer 的 RPC 通信
- 文件生命周期管理
- 数据读写一致性
- 副本机制
- 故障恢复

## 2. 测试环境

| 组件 | 地址 | 说明 |
|------|------|------|
| Master | 127.0.0.1:19090 | 元数据管理 |
| ChunkServer cs1 | 127.0.0.1:19091 | 数据存储节点 |
| ChunkServer cs2 | 127.0.0.1:19092 | 数据存储节点 |
| ChunkServer cs3 | 127.0.0.1:19093 | 数据存储节点 |

使用非标准端口（19090+）避免与其他服务冲突。

## 3. 测试用例

### 3.1 集群启动测试

| 编号 | 用例 | 操作 | 预期结果 |
|------|------|------|----------|
| T01 | Master启动 | 启动Master服务 | 监听19090端口，接受RPC连接 |
| T02 | ChunkServer启动 | 启动3个ChunkServer | 各自监听对应端口 |
| T03 | 心跳注册 | ChunkServer向Master发送heartbeat | Master记录chunkserver状态，返回True |

### 3.2 文件操作测试

| 编号 | 用例 | 操作 | 预期结果 |
|------|------|------|----------|
| T04 | 创建文件 | create_file("/test.txt") | 返回True |
| T05 | 重复创建 | 再次create_file("/test.txt") | 返回False（文件已存在） |
| T06 | 获取文件信息 | get_file_info("/test.txt") | 返回file_name="/test.txt" |
| T07 | 获取不存在文件 | get_file_info("/no_such_file") | 返回空file_name |
| T08 | 删除文件 | delete_file("/test.txt") | 返回True |
| T09 | 删除不存在文件 | delete_file("/no_such_file") | 返回False |

### 3.3 Chunk分配测试

| 编号 | 用例 | 操作 | 预期结果 |
|------|------|------|----------|
| T10 | 分配chunk | get_chunk_handle("/data.bin", 0) | 返回非空chunk_handle |
| T11 | 副本位置 | 检查返回的locations | 包含已注册的chunkserver id |
| T12 | Primary选举 | 检查返回的primary | 非空，为locations中的一个 |
| T13 | 多chunk分配 | get_chunk_handle("/data.bin", 1) | 返回不同的chunk_handle |

### 3.4 数据读写测试

| 编号 | 用例 | 操作 | 预期结果 |
|------|------|------|----------|
| T14 | 写入数据 | write(chunk_handle, 0, "hello") | 返回True |
| T15 | 读取数据 | read(chunk_handle, 0, 5) | 返回"hello" |
| T16 | 偏移写入 | write(chunk_handle, 5, " world") | 返回True |
| T17 | 完整读取 | read(chunk_handle, 0, 11) | 返回"hello world" |
| T18 | 追加数据 | append(chunk_handle, "!") | 无异常 |
| T19 | 读取追加后 | read(chunk_handle, 0, 12) | 返回"hello world!" |
| T20 | 读取空chunk | read("nonexistent", 0, 10) | 返回空字符串 |

### 3.5 快照复制测试

| 编号 | 用例 | 操作 | 预期结果 |
|------|------|------|----------|
| T21 | 复制chunk | copy_snapshot(src_handle, dst_handle) | 无异常 |
| T22 | 验证副本 | read(dst_handle, 0, length) | 数据与源chunk一致 |

### 3.6 Client端到端测试

| 编号 | 用例 | 操作 | 预期结果 |
|------|------|------|----------|
| T23 | Client创建文件 | client.create("/e2e.txt") | 返回True |
| T24 | Client写入 | client.write("/e2e.txt", "end to end") | 返回True |
| T25 | Client读取 | client.read("/e2e.txt") | 返回"end to end" |
| T26 | Client追加 | client.append("/e2e.txt", " test") | 返回True |
| T27 | Client删除 | client.delete("/e2e.txt") | 返回True |

### 3.7 并发与异常测试

| 编号 | 用例 | 操作 | 预期结果 |
|------|------|------|----------|
| T28 | 并发写入 | 多线程同时写入不同文件 | 所有写入成功，数据不混乱 |
| T29 | 并发读写 | 一线程写入，另一线程读取 | 读取到完整数据或空，不会读到部分写入 |
| T30 | ChunkServer不可达 | 关闭一个chunkserver后写入 | stream_recovery跳过故障节点，写入成功 |

## 4. 测试执行方式

```bash
# 自动化测试（启动集群 + 运行所有用例）
python test_cluster.py

# 手动测试（分步启动）
python run.py master --port 19090
python run.py chunkserver --id cs1 --port 19091
python run.py chunkserver --id cs2 --port 19092
python run.py chunkserver --id cs3 --port 19093
```

## 5. 通过标准

- 所有 T01-T27 用例必须通过
- T28-T30 为进阶测试，验证系统健壮性
- 测试脚本输出 `[PASS]` / `[FAIL]` 标记每个用例
- 最终输出通过率统计
