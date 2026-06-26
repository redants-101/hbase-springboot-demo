# HBase Spring Boot Demo

一个面向 HBase 入门学习和实时链路实验的 Spring Boot 示例项目。

项目当前包含三类能力：

1. HBase 基础 REST 接口：建表、写入、读取、扫描、删除。
2. HBase 特性实验接口：表设计、性能参数、高级特性、二级索引实验。
3. Flume -> Kafka -> Flink -> HBase 实时链路验证：通过页面启动 Flink 作业，消费 Kafka 消息并写入 HBase。

配套文章和资料已经放在 `docs/` 目录中，适合按“先 HBase，再实时链路”的顺序阅读。

## 项目结构

```text
hbase-springboot-demo
├── src/main/java/com/example/hbase
│   ├── controller
│   │   ├── HBaseController.java                    # HBase 基础 CRUD 接口
│   │   ├── ExperimentController.java               # HBase 实验接口
│   │   └── KafkaToHBasePipelineController.java     # Kafka -> Flink -> HBase 管道接口
│   ├── service
│   │   └── HBaseService.java                       # HBase 基础操作服务
│   ├── experiments                                # HBase 学习实验
│   │   ├── table                                  # 表设计、TTL、多版本等
│   │   ├── performance                            # BloomFilter、BlockSize、压缩、预分区等
│   │   ├── advanced                               # Filter、Batch、Counter、Snapshot、AsyncClient
│   │   └── index                                  # 二级索引实验
│   └── flink                                      # Kafka -> Flink -> HBase 实时管道
│       ├── config                                 # 管道配置
│       ├── dto                                    # 状态、预览、Kafka 消息 DTO
│       ├── job                                    # Flink Source / Sink / Job
│       └── service                                # 作业生命周期和指标管理
├── src/main/resources
│   ├── application.yml                            # HBase、Kafka、Flink 管道配置
│   ├── logback-spring.xml                         # 日志配置
│   └── static/pipeline.html                       # Kafka 到 HBase 实验控制台
├── docs                                           # 教程和博客资料
├── hadoop_home/bin                                # Windows 本地运行所需 winutils
└── pom.xml                                       # Maven 依赖与打包配置
```

## 技术栈

| 技术 | 版本/说明 |
| --- | --- |
| Java | 17 |
| Spring Boot | 2.7.18 |
| HBase Client | 2.1.3 |
| Hadoop Common | 3.2.4 |
| Flink | 1.18.1 |
| Flink Kafka Connector | 3.1.0-1.18 |
| Maven | 3.8+ |

## 环境要求

运行基础 HBase 接口至少需要：

- JDK 17
- Maven 3.8+
- 可访问的 HBase 2.x 服务
- 可访问的 ZooKeeper

运行完整实时链路还需要：

- Kafka Broker
- Flume 采集进程
- Kafka 中存在目标 topic，默认是 `public-test`
- HBase 网络、端口、ZooKeeper 配置对本机可访问

Windows 环境如果没有配置 `HADOOP_HOME`，项目会默认使用仓库内置的 `hadoop_home/bin/winutils.exe` 和 `hadoop.dll`。

## 快速启动

### 1. 配置 HBase 地址

默认配置在 `src/main/resources/application.yml`：

```yaml
hbase:
  zookeeper:
    quorum: ${HBASE_ZK_QUORUM:140.143.201.112}
    port: ${HBASE_ZK_PORT:2181}
    znode:
      parent: ${HBASE_ZNODE_PARENT:/hbase}
```

建议通过环境变量覆盖，避免把个人服务器地址写死在提交中。

PowerShell 示例：

```powershell
$env:HBASE_ZK_QUORUM="140.143.201.112"
$env:HBASE_ZK_PORT="2181"
$env:HBASE_ZNODE_PARENT="/hbase"
mvn spring-boot:run
```

### 2. 构建并运行

开发时可以直接运行：

```powershell
mvn spring-boot:run
```

也可以打包后运行：

```powershell
mvn clean package
java -jar target/hbase-springboot-demo-1.0.0.jar
```

服务默认启动在 `8080` 端口。

健康检查：

```powershell
curl http://localhost:8080/actuator/health
```

## HBase 基础接口

基础接口统一前缀：

```text
/api/hbase
```

创建表：

```powershell
curl -X POST "http://localhost:8080/api/hbase/table?tableName=test_springboot&columnFamilies=cf1"
```

写入一个单元格：

```powershell
curl -X POST "http://localhost:8080/api/hbase/data" `
  -H "Content-Type: application/json" `
  -d '{"tableName":"test_springboot","rowKey":"row1","columnFamily":"cf1","qualifier":"name","value":"SpringBootHBase"}'
```

读取一个单元格：

```powershell
curl "http://localhost:8080/api/hbase/data?tableName=test_springboot&rowKey=row1&columnFamily=cf1&qualifier=name"
```

读取整行：

```powershell
curl "http://localhost:8080/api/hbase/row?tableName=test_springboot&rowKey=row1"
```

扫描表：

```powershell
curl "http://localhost:8080/api/hbase/scan?tableName=test_springboot"
```

删除行：

```powershell
curl -X DELETE "http://localhost:8080/api/hbase/row?tableName=test_springboot&rowKey=row1"
```

统一成功响应格式：

```json
{
  "code": 0,
  "message": "success",
  "data": "SpringBootHBase"
}
```

## HBase 实验接口

实验接口统一前缀：

```text
/api/experiment
```

列出所有实验：

```powershell
curl "http://localhost:8080/api/experiment/list"
```

运行单个实验：

```powershell
curl "http://localhost:8080/api/experiment/run/1.1"
```

运行整组实验：

```powershell
curl "http://localhost:8080/api/experiment/group/table"
```

清理二级索引实验表：

```powershell
curl "http://localhost:8080/api/experiment/cleanup/index"
```

实验分组：

| 分组 | 说明 |
| --- | --- |
| `table` | 列族属性、多版本、TTL、MIN_VERSIONS、KEEP_DELETED_CELLS |
| `performance` | BloomFilter、BlockSize、压缩、DataBlockEncoding、BlockCache、预分区 |
| `advanced` | Filter、Batch、Counter、Snapshot、AsyncConnection |
| `index` | 单表二级索引、逻辑隔离、物理隔离、前缀扫描、索引扫描对比 |

实验执行详情主要看控制台和日志文件。

## Flume -> Kafka -> Flink -> HBase 链路

当前项目验证的完整链路是：

```text
/var/log/demo/app.log
  -> Flume
  -> Kafka topic: public-test
  -> Spring Boot 内嵌启动 Flink Job
  -> HBase table: flink_public_test
```

说明：

- Flume 和 Kafka 部署不在本项目内启动，项目只负责消费 Kafka 并写入 HBase。
- 当前 Flink 作业以实验方式运行在 Spring Boot 进程内，便于页面控制和学习源码。
- 生产环境通常应将 Flink Job 打包后提交到独立 Flink 集群，而不是长期嵌在 Web 进程中运行。

### 管道配置

默认配置：

```yaml
pipeline:
  kafka-to-hbase:
    bootstrap-servers: ${PIPELINE_KAFKA_BOOTSTRAP_SERVERS:140.143.201.112:9092}
    topic: ${PIPELINE_KAFKA_TOPIC:public-test}
    group-id: ${PIPELINE_KAFKA_GROUP_ID:hbase-springboot-demo-flink}
    start-from-earliest: ${PIPELINE_KAFKA_START_FROM_EARLIEST:false}
    hbase-table: ${PIPELINE_HBASE_TABLE:flink_public_test}
    column-family: ${PIPELINE_HBASE_CF:info}
    parallelism: ${PIPELINE_FLINK_PARALLELISM:1}
    auto-create-table: ${PIPELINE_AUTO_CREATE_TABLE:true}
    preview-size: ${PIPELINE_PREVIEW_SIZE:20}
    checkpoint-interval-ms: ${PIPELINE_CHECKPOINT_INTERVAL:30000}
    min-pause-between-checkpoints-ms: ${PIPELINE_CHECKPOINT_MIN_PAUSE:15000}
    checkpoint-timeout-ms: ${PIPELINE_CHECKPOINT_TIMEOUT:600000}
```

常用环境变量：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PIPELINE_KAFKA_BOOTSTRAP_SERVERS` | `140.143.201.112:9092` | Kafka Broker 地址 |
| `PIPELINE_KAFKA_TOPIC` | `public-test` | Flume 写入、Flink 消费的 topic |
| `PIPELINE_KAFKA_GROUP_ID` | `hbase-springboot-demo-flink` | Kafka 消费者组 |
| `PIPELINE_KAFKA_START_FROM_EARLIEST` | `false` | 是否从 topic 最早位置消费 |
| `PIPELINE_HBASE_TABLE` | `flink_public_test` | HBase 目标表 |
| `PIPELINE_HBASE_CF` | `info` | HBase 目标列族 |
| `PIPELINE_FLINK_PARALLELISM` | `1` | Flink 作业并行度 |
| `PIPELINE_AUTO_CREATE_TABLE` | `true` | 表不存在时是否自动创建 |
| `PIPELINE_PREVIEW_SIZE` | `20` | 页面展示最近写入记录数 |

### 页面操作

启动项目后打开：

```text
http://localhost:8080/pipeline.html
```

页面提供：

- 启动管道
- 停止管道
- 查看运行状态
- 查看 Kafka、HBase、Flink 配置
- 查看消费数、写入数、失败数
- 查看最近写入 HBase 的数据预览
- 展示 JSON 消息解析出的字段

对应接口：

```text
POST /api/pipeline/kafka-hbase/start
POST /api/pipeline/kafka-hbase/stop
GET  /api/pipeline/kafka-hbase/status
```

### 手动写入测试数据

在云服务器上向 Flume 监听的日志文件追加普通文本：

```bash
echo "Hello Flume, this is a test log!" >> /var/log/demo/app.log
```

追加 JSON 日志：

```bash
echo '{"userId":"1001","action":"login","ip":"127.0.0.1","success":true}' >> /var/log/demo/app.log
```

Flink 写入 HBase 时会保存：

| 列 | 说明 |
| --- | --- |
| `raw` | Kafka 消息原文 |
| `kafka_topic` | 来源 topic |
| `kafka_partition` | Kafka 分区 |
| `kafka_offset` | Kafka offset |
| `kafka_timestamp` | Kafka 消息时间戳 |
| JSON 字段名 | 如果消息是 JSON 对象，会额外按字段写入，例如 `userId`、`action`、`ip` |

RowKey 格式：

```text
topic-partition-00000000000000000000
```

示例：

```text
public-test-0-00000000000000000047
```

这个 RowKey 使用 Kafka 的 topic、partition、offset 生成。同一条 Kafka 消息重复处理时会写到同一行，适合实验阶段做幂等验证。

### HBase Shell 验证

进入 HBase Shell 后查看目标表：

```bash
list
scan 'flink_public_test', {LIMIT => 5}
```

查看单行：

```bash
get 'flink_public_test', 'public-test-0-00000000000000000047'
```

## 文档资料

建议阅读顺序：

1. `docs/低配服务器上 HBase 从部署到 Java 访问完整指南.md`
2. `docs/Docker安装Kafka单节点教程.md`
3. `docs/Apache-Flume-安装配置教程.md`
4. `docs/从日志到HBase：Flume Kafka Flink 实时链路入门.md`
5. `docs/HBase 学习路线图（Java程序员·生产级视角）.md`
6. `docs/HBase-Docker-部署手册.md`

其中实时链路博客对应的主线是：

```text
log -> Flume -> Kafka -> Flink -> HBase
```

## 测试说明

`HBaseServiceIT` 是集成测试，需要真实可访问的 HBase 集群。

该测试类使用 `IT` 后缀命名，不会被 Maven Surefire 默认的 `mvn test` 执行。日常开发可以先运行：

```powershell
mvn test
```

如需运行集成测试，应确认 HBase 配置正确后由 IDE 或 Maven 集成测试配置单独执行。

## 注意事项

- `.vscode/`、`.idea/`、`target/` 等本地开发文件不应提交到仓库。
- 本项目中的 Flink 管道是实验验证实现，适合学习 Kafka Source、Flink Job、HBase Sink 的代码结构。
- 生产环境建议使用独立 Flink 集群提交 Job，并配置持久化 Checkpoint、Savepoint、监控告警和资源隔离。
- HBase 连接参数、Kafka 地址、消费者组、目标表名都建议使用环境变量覆盖。
- 如果页面显示 Kafka 已消费但 HBase 没有数据，优先检查 HBase 连接、目标表列族、Flink 失败日志和页面 `failedCount/lastError`。
