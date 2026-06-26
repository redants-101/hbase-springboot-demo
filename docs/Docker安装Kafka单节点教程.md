# 🚀 Docker 安装 Kafka 单节点教程（小白友好版）

> 适用系统：Ubuntu / CentOS / Debian 等 Linux 系统  
> 模式：KRaft（无需 Zookeeper，单节点即开即用）  
> 用途：教学、开发测试、本地实验

---

## 📋 前置条件

在开始之前，请确保你的服务器已安装 Docker。

### 检查 Docker 是否已安装

```bash
docker --version
```

如果显示版本号（如 `Docker version 27.5.1`），说明已安装，可以跳过安装步骤。

### 安装 Docker（如未安装）

```bash
# 一键安装 Docker
curl -fsSL https://get.docker.com | bash

# 启动 Docker 并设置开机自启
systemctl start docker
systemctl enable docker

# 验证安装
docker --version
```

---

## 🎯 第一步：部署 Kafka 容器

执行下面这条命令，一条命令完成 Kafka 部署：

```bash
docker run -d \
  --name kafka \
  --restart=always \
  -p 9092:9092 \
  -e KAFKA_CFG_NODE_ID=1 \
  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://你的服务器IP:9092 \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  bitnami/kafka:latest
```

> ⚠️ **重要**：请把 `你的服务器IP` 替换成你服务器的真实公网 IP 地址！

### 参数说明（小白可跳过）

| 参数 | 作用 |
|------|------|
| `--name kafka` | 给容器起个名字叫 kafka |
| `--restart=always` | 服务器重启或 Docker 重启后，容器自动启动 |
| `-p 9092:9092` | 把容器的 9092 端口映射到服务器 |
| `KAFKA_CFG_NODE_ID=1` | 节点编号（单节点填 1） |
| `KAFKA_CFG_PROCESS_ROLES=controller,broker` | 同时充当 controller 和 broker |
| `ADVERTISED_LISTENERS` | 对外暴露的连接地址 |

---

## ✅ 第二步：验证 Kafka 是否启动成功

### 2.1 查看容器状态

```bash
docker ps --filter name=kafka
```

看到 `STATUS` 列显示 `Up` 就说明容器在运行。

### 2.2 查看启动日志

```bash
docker logs kafka --tail 20
```

看到 `Kafka Server started` 就说明启动成功！

### 2.3 检查端口

```bash
ss -tlnp | grep 9092
```

看到 `LISTEN` 说明端口正在监听。

---

## 🧪 第三步：测试 Kafka 功能

### 3.1 创建一个 Topic（主题）

```bash
docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic my-first-topic \
  --partitions 1 \
  --replication-factor 1
```

看到 `Created topic my-first-topic.` 就成功了！

> 💡 **什么是 Topic？** 可以理解为一个消息的"文件夹"，生产者往里面发消息，消费者从里面读消息。

### 3.2 发送一条消息（生产）

```bash
echo '你好，Kafka！这是我的第一条消息' | docker exec -i kafka kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-first-topic
```

### 3.3 读取消息（消费）

```bash
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-first-topic \
  --from-beginning \
  --max-messages 1
```

如果看到你刚才发送的消息内容，说明 Kafka 工作正常！🎉

---

## 🔧 常用管理命令

```bash
# 查看所有 Topic
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# 查看某个 Topic 的详情
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic my-first-topic

# 删除 Topic
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic my-first-topic

# 持续消费消息（实时监听新消息）
docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my-first-topic --from-beginning

# 查看容器日志
docker logs kafka --tail 50

# 实时查看日志
docker logs -f kafka

# 重启 Kafka
docker restart kafka

# 停止 Kafka
docker stop kafka

# 启动已停止的 Kafka
docker start kafka

# 完全删除容器（数据会丢失！）
docker stop kafka && docker rm kafka

# 查看容器重启策略
docker inspect kafka --format '{{.HostConfig.RestartPolicy.Name}}'

# 修改已有容器的重启策略为 always（无需重建容器）
docker update --restart=always kafka
```

### 🔄 容器自动重启说明

部署命令中已包含 `--restart=always`，这意味着：

| 场景 | 容器行为 |
|------|----------|
| 服务器重启 | ✅ 自动启动 |
| Docker 服务重启 | ✅ 自动启动 |
| 容器异常退出 | ✅ 自动重启 |
| 手动 `docker stop` | ❌ 不会自动启动（需手动 `docker start`） |

> 💡 如果你之前创建容器时忘了加 `--restart=always`，可以用 `docker update --restart=always kafka` 补救，无需重建容器。

---

## 🌐 外部连接

如果你想从本地电脑或其他服务器连接 Kafka，使用以下地址：

```
你的服务器公网IP:9092
```

例如：`140.143.201.112:9092`

### Python 客户端示例

```python
from kafka import KafkaProducer, KafkaConsumer

# 生产者 - 发送消息
producer = KafkaProducer(bootstrap_servers='你的服务器IP:9092')
producer.send('my-first-topic', b'Hello from Python!')
producer.flush()
print('消息发送成功！')

# 消费者 - 接收消息
consumer = KafkaConsumer(
    'my-first-topic',
    bootstrap_servers='你的服务器IP:9092',
    auto_offset_reset='earliest'
)
for msg in consumer:
    print(f'收到消息: {msg.value.decode()}')
    break
```

---

## ❓ 常见问题

### Q1：端口 9092 连不上？

检查防火墙是否开放了 9092 端口：

```bash
# 腾讯云/阿里云等云服务器，需要在安全组中放行 9092 端口
# 本地防火墙放行（如有）
ufw allow 9092
```

### Q2：容器启动后马上退出？

查看日志排查原因：

```bash
docker logs kafka
```

### Q3：如何升级 Kafka 版本？

```bash
# 拉取最新镜像
docker pull bitnami/kafka:latest

# 删除旧容器并重新创建
docker stop kafka && docker rm kafka
# 然后重新执行第一步的 docker run 命令
```

### Q4：数据存在哪里？

默认存在容器内部。如果需要持久化到服务器磁盘，在 `docker run` 时添加：

```bash
-v /data/kafka:/bitnami/kafka/data
```

### Q5：服务器重启后 Kafka 没有自动启动？

检查容器的重启策略：

```bash
docker inspect kafka --format '{{.HostConfig.RestartPolicy.Name}}'
```

- 如果显示 `no`，说明未启用自动重启，执行 `docker update --restart=always kafka` 即可修复
- 如果显示 `always`，说明已启用，检查 Docker 服务本身是否开机自启：`systemctl is-enabled docker`

---

## 📊 架构简图

```
┌─────────────────────────────────────┐
│           Docker 容器 (kafka)        │
│                                     │
│  ┌──────────┐    ┌──────────────┐   │
│  │Controller│◄──►│   Broker     │   │
│  │  :9093   │    │   :9092      │   │
│  └──────────┘    └──────┬───────┘   │
│                         │           │
└─────────────────────────┼───────────┘
                          │
                    ┌─────▼─────┐
                    │  客户端    │
                    │ (生产者/消费者)│
                    └───────────┘
```

> 单节点模式下，Controller 和 Broker 合二为一，无需 Zookeeper，架构极简。

---

## 🎉 总结

恭喜！你已经成功用 Docker 部署了一个 Kafka 单节点实例。回顾一下你学会了：

1. ✅ 用一条命令部署 Kafka
2. ✅ 创建 Topic
3. ✅ 发送和接收消息
4. ✅ 管理 Kafka 容器

现在你可以开始用 Kafka 做消息队列实验了！🚀
