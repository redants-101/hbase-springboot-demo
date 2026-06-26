# Apache Flume 1.11.0 安装配置教程

> 📅 创建时间：2026-06-24  
> 🖥️ 服务器：Ubuntu 24.04 | 140.143.201.112  
> ☕ Java 版本：OpenJDK 17  
> 📦 Flume 版本：1.11.0

---

## 一、什么是 Apache Flume？

Apache Flume 是一个**分布式、可靠的数据采集系统**，专门用于高效地收集、聚合和移动大量日志数据。

简单来说，它的工作流程是：

```
数据源（Source）→ 缓冲区（Channel）→ 目的地（Sink）
```

举个例子：你想把服务器上的日志实时发送到 Kafka 或 HBase，Flume 就是干这个的。

---

## 二、环境准备

### 2.1 检查 Java 环境

Flume 依赖 Java 运行，先确认 Java 已安装：

```bash
java -version
```

本教程使用 OpenJDK 17，路径为 `/usr/lib/jvm/java-17-openjdk-amd64`。

如果未安装 Java：

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk
```

---

## 三、下载并安装 Flume

### 3.1 下载 Flume

从清华镜像站下载（国内速度快）：

```bash
cd /tmp
wget https://mirrors.tuna.tsinghua.edu.cn/apache/flume/1.11.0/apache-flume-1.11.0-bin.tar.gz
```

### 3.2 解压安装

```bash
sudo tar -xzf apache-flume-1.11.0-bin.tar.gz -C /opt
```

解压后安装目录为 `/opt/apache-flume-1.11.0-bin`。

---

## 四、配置环境变量

创建环境变量文件，让系统能找到 Flume 命令：

```bash
sudo tee /etc/profile.d/flume.sh << 'EOF'
export FLUME_HOME=/opt/apache-flume-1.11.0-bin
export PATH=$PATH:$FLUME_HOME/bin
EOF
```

使配置生效：

```bash
source /etc/profile.d/flume.sh
```

验证安装：

```bash
flume-ng version
```

应该看到 `Flume 1.11.0` 的版本信息。

---

## 五、创建数据管道配置

### 5.1 理解 Flume 配置结构

一个 Flume Agent 由三部分组成：

| 组件 | 作用 | 本教程使用 |
|------|------|------------|
| **Source** | 从哪里接收数据 | NetCat（监听网络端口） |
| **Channel** | 数据缓冲区 | Memory（内存通道） |
| **Sink** | 数据发往哪里 | Logger（输出到日志） |

### 5.2 创建配置文件

```bash
sudo tee /opt/apache-flume-1.11.0-bin/conf/flume-conf.properties << 'EOF'
# Agent 名称
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Source 配置：监听 44444 端口接收数据
a1.sources.r1.type = netcat
a1.sources.r1.bind = 0.0.0.0
a1.sources.r1.port = 44444

# Channel 配置：内存通道，容量 1000 条
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

# Sink 配置：输出到日志
a1.sinks.k1.type = logger

# 绑定：Source → Channel → Sink
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
EOF
```

---

## 六、配置日志输出

### 6.1 创建日志目录

```bash
sudo mkdir -p /opt/apache-flume-1.11.0-bin/logs
```

### 6.2 修改日志配置

编辑 `/opt/apache-flume-1.11.0-bin/conf/log4j2.xml`，找到这一行：

```xml
<Property name="LOG_DIR">.</Property>
```

改为：

```xml
<Property name="LOG_DIR">/opt/apache-flume-1.11.0-bin/logs</Property>
```

这样日志就会统一输出到 Flume 安装目录下的 `logs` 文件夹。

---

## 七、配置 systemd 开机自启

### 7.1 创建服务文件

```bash
sudo tee /etc/systemd/system/flume.service << 'EOF'
[Unit]
Description=Apache Flume Agent Service
Documentation=https://flume.apache.org/
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/apache-flume-1.11.0-bin
Environment=FLUME_HOME=/opt/apache-flume-1.11.0-bin
Environment=JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ExecStart=/opt/apache-flume-1.11.0-bin/bin/flume-ng agent -n a1 -c /opt/apache-flume-1.11.0-bin/conf -f /opt/apache-flume-1.11.0-bin/conf/flume-conf.properties
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF
```

### 7.2 启动服务

```bash
# 重载 systemd 配置
sudo systemctl daemon-reload

# 启动 Flume
sudo systemctl start flume

# 设置开机自启
sudo systemctl enable flume

# 查看状态
sudo systemctl status flume
```

看到 `active (running)` 就说明启动成功了。

---

## 八、验证数据管道

### 8.1 检查端口监听

```bash
ss -tlnp | grep 44444
```

应该看到端口 44444 正在监听。

### 8.2 发送测试数据

```bash
echo 'Hello Flume!' | nc 127.0.0.1 44444
```

返回 `OK` 表示数据发送成功。

### 8.3 查看日志确认

```bash
tail -f /opt/apache-flume-1.11.0-bin/logs/flume.log
```

你应该能看到类似这样的输出：

```
Event: { headers:{} body: 48 65 6C 6C 6F 20 46 6C 75 6D 65 Hello Flume }
```

这说明数据已经成功通过 Flume 管道了！🎉

---

## 九、Exec Source + Kafka Sink 实战

上一节我们用了 NetCat Source + Logger Sink 做入门演示。在实际生产环境中，最常见的场景是**实时采集日志文件并推送到 Kafka**。本节将配置 Exec Source 监控日志文件，Kafka Sink 将数据推送到 Kafka 主题。

### 9.1 场景说明

```
/var/log/demo/app.log  ──→  Flume Exec Source  ──→  Memory Channel  ──→  Kafka Sink  ──→  Kafka Topic: public-test
```

*   **Source**：Exec Source，使用 `tail -F` 命令实时监控日志文件
    
*   **Channel**：Memory Channel，内存缓冲区
    
*   **Sink**：Kafka Sink，将数据推送到 Kafka 集群
    

### 9.2 准备日志目录

```bash
sudo mkdir -p /var/log/demo
```

### 9.3 创建 Kafka 配置文件

```bash
sudo tee /opt/apache-flume-1.11.0-bin/conf/flume-kafka.properties << 'EOF'
# ============================================
# Flume Exec Source + Kafka Sink 配置
# 功能：实时采集 /var/log/demo/app.log 并推送到 Kafka
# ============================================

# Agent 名称
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# ---------- Source：Exec Source ----------
# 使用 tail -F 实时监控日志文件（文件轮转后自动跟踪新文件）
a1.sources.r1.type = exec
a1.sources.r1.command = tail -F /var/log/demo/app.log
a1.sources.r1.channels = c1

# ---------- Channel：Memory Channel ----------
# 内存通道，高性能但重启会丢失未消费数据
a1.channels.c1.type = memory
a1.channels.c1.capacity = 10000
a1.channels.c1.transactionCapacity = 1000

# ---------- Sink：Kafka Sink ----------
# 将数据推送到 Kafka 集群
a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
a1.sinks.k1.kafka.topic = public-test
a1.sinks.k1.kafka.bootstrap.servers = 140.143.201.112:9092
a1.sinks.k1.kafka.producer.acks = 1
a1.sinks.k1.flumeBatchSize = 20
a1.sinks.k1.useFlumeEventFormat = false
a1.sinks.k1.channel = c1
EOF
```

**关键参数说明：**

| 参数 | 说明 |
|------|------|
| `a1.sources.r1.type = exec` | 使用 Exec Source，执行指定命令采集数据 |
| `a1.sources.r1.command = tail -F ...` | 使用 `tail -F` 实时跟踪日志文件（`-F` 会在文件轮转后自动重连） |
| `a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink` | Kafka Sink 的完整类名 |
| `a1.sinks.k1.kafka.bootstrap.servers` | Kafka 集群地址（**必须使用宿主机 IP，不能用 localhost**） |
| `a1.sinks.k1.kafka.topic` | 目标 Kafka 主题 |
| `a1.sinks.k1.flumeBatchSize` | 每批次发送的消息数量 |
| `a1.sinks.k1.useFlumeEventFormat = false` | 关闭 Flume 事件格式封装，避免 Kafka 消费端出现乱码 |

### 9.4 更新 systemd 服务配置

将 `ExecStart` 中的配置文件路径改为 `flume-kafka.properties`：

```bash
sudo sed -i 's|flume-conf.properties|flume-kafka.properties|g' /etc/systemd/system/flume.service
```

修改后的 `ExecStart` 行应为：

```
ExecStart=/opt/apache-flume-1.11.0-bin/bin/flume-ng agent -n a1 -c /opt/apache-flume-1.11.0-bin/conf -f /opt/apache-flume-1.11.0-bin/conf/flume-kafka.properties
```

### 9.5 重启服务

```bash
sudo systemctl daemon-reload
sudo systemctl restart flume
sudo systemctl status flume
```

### 9.6 验证管道

**步骤 1：确认 Flume 服务正常运行**

```bash
sudo systemctl status flume
```

**步骤 2：确认 tail 进程正在监控日志文件**

```bash
ps aux | grep 'tail -F /var/log/demo/app.log'
```

**步骤 3：向日志文件追加测试数据**

```bash
echo "[$(date '+%Y-%m-%d %H:%M:%S')] INFO - Test message from Flume" | sudo tee -a /var/log/demo/app.log
```

**步骤 4：在 Kafka 消费者中验证**

```bash
# 进入 Kafka 容器
sudo docker exec -it kafka bash

# 消费 public-test 主题的消息
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic public-test --from-beginning
```

如果看到刚才追加的日志消息，说明 Exec Source + Kafka Sink 管道配置成功！🎉

### 9.7 常见问题排查

| 问题 | 可能原因 | 解决方法 |
|------|----------|----------|
| Flume 启动失败 | 配置文件语法错误 | 检查 `flume-kafka.properties` 缩进和参数名 |
| Kafka 连接失败 | bootstrap.servers 地址错误 | 确认使用宿主机 IP（非 localhost） |
| 日志文件无数据 | 文件路径不存在 | `sudo mkdir -p /var/log/demo && sudo touch /var/log/demo/app.log` |
| 消费者读不到消息 | topic 不存在 | Kafka 默认自动创建 topic，检查 Kafka 是否正常运行 |
| Kafka 消费出现乱码 | `useFlumeEventFormat` 未关闭 | 在 Kafka Sink 配置中添加 `a1.sinks.k1.useFlumeEventFormat = false` |

---

## 十、常用管理命令

```bash
# 启动服务
sudo systemctl start flume

# 停止服务
sudo systemctl stop flume

# 重启服务
sudo systemctl restart flume

# 查看状态
sudo systemctl status flume

# 查看实时日志
sudo journalctl -u flume -f

# 查看 Flume 自身日志
tail -f /opt/apache-flume-1.11.0-bin/logs/flume.log

# 禁用开机自启
sudo systemctl disable flume
```

---

## 十一、关键文件速查

| 文件 | 路径 |
|------|------|
| 安装目录 | `/opt/apache-flume-1.11.0-bin` |
| 管道配置（NetCat） | `/opt/apache-flume-1.11.0-bin/conf/flume-conf.properties` |
| 管道配置（Exec+Kafka） | `/opt/apache-flume-1.11.0-bin/conf/flume-kafka.properties` |
| 日志配置 | `/opt/apache-flume-1.11.0-bin/conf/log4j2.xml` |
| 日志文件 | `/opt/apache-flume-1.11.0-bin/logs/flume.log` |
| systemd 服务 | `/etc/systemd/system/flume.service` |
| 环境变量 | `/etc/profile.d/flume.sh` |
| 监控日志目录 | `/var/log/demo/` |

---

## 十二、下一步可以做什么？

1.  **接入 HBase**：将 Sink 改为 HBase Sink，把数据写入 HBase 表
    
2.  **多 Agent 级联**：多个 Flume Agent 串联，构建复杂数据管道
    
3.  **使用 SpoolDir Source**：监控目录，自动采集新增文件（比 Exec 更可靠）
    
4.  **使用 Taildir Source**：监控多个文件，支持断点续传（生产环境推荐）
    
5.  **配置多个 Channel**：使用 File Channel 保证数据不丢失
    

---

> 💡 提示：本教程基于实际部署经验编写，所有步骤均已在 Ubuntu 24.04 上验证通过。