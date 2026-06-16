# 低配服务器上 HBase 从部署到 Java 访问完整指南

> 背景：本文面向还没有 HBase 实战经验的 Java 程序员。目标是在一台低配云服务器上用 Docker 跑起单机 HBase，再用本地 Spring Boot 项目完成创建表、写入、查询、扫描和删除等基础操作。

本文配套代码是 [`redants-101/hbase-springboot-demo`](https://github.com/redants-101/hbase-springboot-demo)。这篇文章重点讲“为什么这么配、怎么连通、哪里容易踩坑”，完整代码以仓库为准。

和代码仓库的对应关系如下：

| 文章部分 | 代码位置 |
| --- | --- |
| HBase 连接参数 | `src/main/resources/application.yml` |
| Windows 本地运行配置 | `src/main/java/com/example/hbase/HbaseSpringbootApplication.java` |
| HBase 单例连接 | `src/main/java/com/example/hbase/config/HBaseConfig.java` |
| HBase 操作封装 | `src/main/java/com/example/hbase/service/impl/HBaseServiceImpl.java` |
| REST 接口 | `src/main/java/com/example/hbase/controller/HBaseController.java` |
| 参数校验和异常返回 | `src/main/java/com/example/hbase/exception/GlobalExceptionHandler.java` |

完成本文后，你会得到：

- 一台低配服务器上的单机 HBase 环境。
- 一个本地可运行的 Spring Boot 示例项目。
- 一套能通过 REST 接口访问 HBase 的最小闭环。
- 对端口、安全组、`UnknownHostException`、低内存启动失败等问题的基本排查能力。

---

## 一、环境准备

### 1.1 服务器与本地环境

本文示例环境：

| 项目 | 示例值 | 说明               |
| --- | --- |------------------|
| 云服务器 | 低配云服务器 | 2G 到 4G 内存均可尝试   |
| 操作系统 | Linux | 示例命令以 Linux 为主   |
| Docker | 26.x | 已安装并启动           |
| HBase 镜像 | `harisekhon/hbase:latest` | 与部署手册保持一致        |
| 本地开发机 | Windows / macOS / Linux | 运行 Spring Boot 项目 |
| JDK | 17 | 与当前 demo 保持一致    |
| Spring Boot | 2.7.18 | 与当前 demo 保持一致    |

下文使用 `140.143.201.112` 作为服务器公网 IP 示例。你实际操作时需要替换为自己的服务器公网 IP。

### 1.2 确认 Docker 可用

```bash
docker --version
systemctl status docker
```

如果 Docker 没启动：

```bash
sudo systemctl enable docker
sudo systemctl start docker
```

### 1.3 Docker Hub 拉取慢的处理

低配云服务器经常遇到 Docker Hub 拉取超时：

```text
Get https://registry-1.docker.io/v2/: net/http: request canceled
```

可以配置镜像加速器：

```bash
sudo mkdir -p /etc/docker

sudo tee /etc/docker/daemon.json <<-'EOF'
{
  "registry-mirrors": ["https://xxxxx.mirror.aliyuncs.com"]
}
EOF

sudo systemctl daemon-reload
sudo systemctl restart docker
```

然后拉取镜像：

```bash
docker pull harisekhon/hbase:latest
docker images | grep hbase
```

---

## 二、HBase 单机容器部署

这一章的部署参数与《HBase-Docker-部署手册.md》保持一致。主博客只保留教学闭环需要的核心步骤，更完整的运维命令、数据持久化和排查清单可以看那份部署手册。

### 2.1 端口先讲清楚

Java 客户端连接 HBase 时，不是只连一个 Web 页面端口。它会先连 ZooKeeper，再根据 ZooKeeper 中注册的信息访问 Master 和 RegionServer。

| 端口 | 作用 | 本文是否建议开放 |
| --- | --- | --- |
| `2181` | ZooKeeper 客户端连接端口 | 必须 |
| `16000` | HBase Master RPC | 建议开放，DDL 操作容易用到 |
| `16010` | HBase Master Web UI | 建议开放，方便观察 |
| `16020` | RegionServer RPC | 必须，读写数据会用到 |
| `16030` | RegionServer Web UI | 建议开放，方便观察 |
| `9090` | Thrift Server | 可选 |
| `9095` | REST Server | 可选 |

如果你只是跟着本文跑 Spring Boot demo，重点关注 `2181`、`16000`、`16010`、`16020`、`16030`。

### 2.2 创建 HBase 容器

先按部署手册的标准端口映射创建容器：

```bash
docker run -d \
  --name hbase \
  --hostname hbase \
  --restart always \
  -p 2181:2181 \
  -p 16000:16000 \
  -p 16010:16010 \
  -p 16020:16020 \
  -p 16030:16030 \
  -p 9090:9090 \
  -p 9095:9095 \
  harisekhon/hbase:latest
```

低配服务器如果启动后很快退出，先看日志：

```bash
docker logs hbase --tail 100
```

如果出现类似内存不足的错误：

```text
library initialization failed - unable to allocate file descriptor table - out of memory
```

先确认宿主机内存：

```bash
free -h
```

1.6Gi 左右的机器跑 HBase 单机容器会比较吃紧。学习阶段可以先关闭其他占内存服务；如果仍然不稳定，建议升级到 2Gi 以上。低内存参数可以作为临时尝试，但不建议当作稳定方案长期依赖。

如果你只是为了跟着本文理解 Java 访问链路，低配服务器能跑起来即可；如果你准备长期保留数据，建议参考《HBase-Docker-部署手册.md》里的数据卷挂载方案。本文主线不展开持久化，是为了让第一次学习的路径更短。

### 2.3 修改 hbase-site.xml

这是跨网络访问 HBase 最容易踩坑的地方。

如果 Spring Boot 项目在你的本地电脑上，而 HBase 容器在云服务器上，那么 HBase 不能把容器内部 hostname 注册给 ZooKeeper。否则本地 Java 客户端会拿到类似 `hbase` 或容器 ID 的地址，最终报：

```text
UnknownHostException
```

进入容器修改配置：

```bash
docker exec -it hbase bash
vi /hbase/conf/hbase-site.xml
```

把关键配置调整为下面这样。注意把 `140.143.201.112` 替换为你的服务器公网 IP：

```xml
<configuration>
  <property>
    <name>hbase.master.ipc.address</name>
    <value>0.0.0.0</value>
  </property>
  <property>
    <name>hbase.regionserver.ipc.address</name>
    <value>0.0.0.0</value>
  </property>
  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>140.143.201.112</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.clientPort</name>
    <value>2181</value>
  </property>
  <property>
    <name>hbase.master.hostname</name>
    <value>140.143.201.112</value>
  </property>
  <property>
    <name>hbase.regionserver.hostname</name>
    <value>140.143.201.112</value>
  </property>
</configuration>
```

这里有一个很好记的规则：

| 配置项 | 作用 | 推荐值 |
| --- | --- | --- |
| `hbase.master.ipc.address` | Master 监听哪个网卡 | `0.0.0.0` |
| `hbase.regionserver.ipc.address` | RegionServer 监听哪个网卡 | `0.0.0.0` |
| `hbase.master.hostname` | Master 对外宣告什么地址 | 服务器公网 IP |
| `hbase.regionserver.hostname` | RegionServer 对外宣告什么地址 | 服务器公网 IP |

简单记：`ipc.address` 管绑定，`hostname` 管宣告。

修改后退出容器并重启：

```bash
docker restart hbase
```

等待 30 到 60 秒后查看日志：

```bash
docker logs hbase --tail 100
```

看到 Master、RegionServer 正常启动后，再继续下一步。

### 2.4 配置云防火墙和系统防火墙

端口访问问题优先排查顺序：

```text
云平台安全组/防火墙 > 系统防火墙 > 容器端口监听
```

云平台安全组至少放行：

```text
2181, 16000, 16010, 16020, 16030
```

如果你也要测试 Thrift 或 REST Server，再放行：

```text
9090, 9095
```

安全提醒：`0.0.0.0/0` 只适合临时测试。个人学习可以短期开启，测试完成后建议改成只允许自己的本地公网 IP。HBase 和 ZooKeeper 不建议长期裸露在公网。

更稳妥的做法是：先临时放行自己的本地公网 IP，确认 Java 客户端能访问；如果排查时短暂使用 `0.0.0.0/0`，测试完成后立刻收回。

如果服务器启用了 `ufw`，也需要放行：

```bash
sudo ufw allow 2181/tcp
sudo ufw allow 16000/tcp
sudo ufw allow 16010/tcp
sudo ufw allow 16020/tcp
sudo ufw allow 16030/tcp
```

本地电脑验证端口：

```bash
telnet 140.143.201.112 2181
curl -s http://140.143.201.112:16010/master-status | head -5
```

服务器上检查监听：

```bash
ss -tlnp | grep -E '2181|16000|16010|16020|16030'
docker exec hbase jps
```

---

## 三、HBase Shell 基本操作

进入 HBase Shell：

```bash
docker exec -it hbase hbase shell
```

为了和后面的 Spring Boot 接口保持一致，本文统一使用：

| 项目 | 值 |
| --- | --- |
| 表名 | `test_springboot` |
| 列族 | `cf1` |
| rowKey | `row1` |
| qualifier | `name` |

常用命令：

```sql
create 'test_springboot', 'cf1'

put 'test_springboot', 'row1', 'cf1:name', 'Alice'

get 'test_springboot', 'row1'

get 'test_springboot', 'row1', {COLUMN => 'cf1:name'}

scan 'test_springboot'

deleteall 'test_springboot', 'row1'
```

常见错误：

```text
ERROR: Unknown table test_springbootrow1!
```

原因通常是命令漏了逗号。正确写法是：

```sql
get 'test_springboot', 'row1'
```

---

## 四、Spring Boot 集成 HBase

这一章不是“生产环境直接可用方案”，而是一个结构比较清楚的学习级 demo。它能帮助你理解 Java 客户端访问 HBase 的基本链路。

项目核心调用链：

```text
application.yml
  -> HBaseConfig 创建 Connection
  -> HBaseService 定义能力
  -> HBaseServiceImpl 调用 HBase API
  -> HBaseController 暴露 REST 接口
  -> GlobalExceptionHandler 统一异常响应
```

### 4.1 Maven 依赖

当前 demo 使用：

- Spring Boot `2.7.18`
- JDK `17`
- HBase Client `2.1.3`
- Hadoop Common `3.2.4`

核心依赖以仓库中的 `pom.xml` 为准。这里需要特别注意两点：

- 排除 HBase/Hadoop 传递进来的旧日志实现，避免和 Spring Boot 日志体系冲突。
- Windows 本地开发时，项目内置了 `hadoop_home/bin/winutils.exe` 和 `hadoop.dll`，用于降低本地运行门槛。

如果你只想跟着文章先跑通流程，可以先把 `HBASE_ZK_QUORUM`、`HBASE_ZK_PORT` 这些参数理解为“本地电脑连远程 HBase 的入口”。真正写代码时，再回到仓库里的 `application.yml` 和 `HBaseConfig` 看具体实现。

### 4.2 application.yml

当前项目支持通过环境变量覆盖 HBase 连接参数：

```yaml
hbase:
  zookeeper:
    quorum: ${HBASE_ZK_QUORUM:localhost}
    port: ${HBASE_ZK_PORT:2181}
    znode:
      parent: ${HBASE_ZNODE_PARENT:/hbase}
  client:
    retries: ${HBASE_CLIENT_RETRIES:3}
    operation-timeout: ${HBASE_OPERATION_TIMEOUT:10000}
    rpc-timeout: ${HBASE_RPC_TIMEOUT:60000}
    scanner-caching: ${HBASE_SCANNER_CACHING:100}
```

建议你本地运行时显式设置环境变量：

```powershell
$env:HBASE_ZK_QUORUM="140.143.201.112"
$env:HBASE_ZK_PORT="2181"
mvn spring-boot:run
```

如果你不设置环境变量，项目默认连接 `localhost`。本文这种“本地电脑访问云服务器 HBase”的场景，需要通过环境变量把 ZooKeeper 地址改成服务器公网 IP。

### 4.3 HBaseConfig：管理单例 Connection

HBase 的 `Connection` 是重量级对象，适合在 Spring 容器里创建一个单例 Bean。当前项目中：

- `hbaseConfiguration()` 负责设置 ZooKeeper、超时、扫描缓存等参数。
- `hbaseConnection()` 负责创建 HBase `Connection`。
- `@Bean(destroyMethod = "close")` 确保应用关闭时释放连接。

关键思想是：`Connection` 复用，`Table` 每次使用后关闭。

当前实现里还做了一件小事：Windows 环境下如果没有配置 `HADOOP_HOME`，会优先使用项目内置的 `hadoop_home`。这也是为什么本地在 Windows 上跑 demo 比较省心。

### 4.4 HBaseServiceImpl：封装 HBase API

当前 demo 支持这些操作：

| 方法 | 作用 |
| --- | --- |
| `createTable` | 创建表 |
| `putData` | 写入一个单元格 |
| `getData` | 读取一个单元格 |
| `getRow` | 读取整行 |
| `scanTable` | 扫描表 |
| `deleteRow` | 删除整行 |

这里有一个重要设计：`getData` 只负责读取明确的 `columnFamily + qualifier`。如果想读取整行，走 `getRow`。这样接口语义更清楚，也避免“传了列族但没传列名”时行为含糊。

在当前代码里：

- `createTable(tableName, columnFamilies...)` 会先检查列族是否为空。
- `putData(...)` 不接受 `null` 值。
- `getData(...)` 必须同时传 `columnFamily` 和 `qualifier`。
- `scanTable(...)` 支持可选的起止行。
- `deleteRow(...)` 删除整行。

这些约束都在服务层做掉了，Controller 只负责接收参数并把请求交给服务层。

### 4.5 REST 接口

当前 Controller 的基础路径是：

```text
/api/hbase
```

接口列表：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/hbase/table` | 创建表 |
| `POST` | `/api/hbase/data` | 写入单元格 |
| `GET` | `/api/hbase/data` | 查询单元格 |
| `GET` | `/api/hbase/row` | 查询整行 |
| `GET` | `/api/hbase/scan` | 扫描表 |
| `DELETE` | `/api/hbase/row` | 删除整行 |

查询单元格时参数名是 `columnFamily` 和 `qualifier`，不是 `cf` 和 `col`。

这点和文章前面的 Shell 示例保持一致，但参数名更贴近当前仓库实现。读者按本文测试时，直接照着这两个字段名传就行。

### 4.6 接口测试

创建表：

```bash
curl -X POST "http://localhost:8080/api/hbase/table?tableName=test_springboot&columnFamilies=cf1"
```

写入一个单元格：

```bash
curl -X POST "http://localhost:8080/api/hbase/data" \
  -H "Content-Type: application/json" \
  -d '{"tableName":"test_springboot","rowKey":"row1","columnFamily":"cf1","qualifier":"name","value":"Alice"}'
```

读取一个单元格：

```bash
curl "http://localhost:8080/api/hbase/data?tableName=test_springboot&rowKey=row1&columnFamily=cf1&qualifier=name"
```

读取整行：

```bash
curl "http://localhost:8080/api/hbase/row?tableName=test_springboot&rowKey=row1"
```

扫描表：

```bash
curl "http://localhost:8080/api/hbase/scan?tableName=test_springboot"
```

删除行：

```bash
curl -X DELETE "http://localhost:8080/api/hbase/row?tableName=test_springboot&rowKey=row1"
```

### 4.7 运行前后的验证顺序

建议按这个顺序走，最不容易乱：

1. 先按第二章把 HBase 容器启动起来。
2. 用 `docker logs hbase --tail 100` 确认 Master 正常初始化。
3. 用 HBase Shell 建表并插入一条数据。
4. 再启动 `hbase-springboot-demo`。
5. 先调用 `GET /api/hbase/row` 和 `GET /api/hbase/data`，确认 Java 客户端能读。
6. 最后再测 `POST /api/hbase/data` 和 `DELETE /api/hbase/row`，确认写和删都正常。

如果 Java 访问失败，不要立刻怀疑代码。优先回到第二章检查 `hostname`、端口和安全组。

---

## 五、常见问题与解决方案

### 5.1 UnknownHostException

现象：

```text
UnknownHostException: hbase
UnknownHostException: 容器ID
```

原因：HBase 向 ZooKeeper 注册的是容器内部 hostname，本地 Java 客户端无法解析。

解决：

- `hbase.master.hostname` 设置为服务器公网 IP。
- `hbase.regionserver.hostname` 设置为服务器公网 IP。
- `hbase.master.ipc.address` 设置为 `0.0.0.0`。
- `hbase.regionserver.ipc.address` 设置为 `0.0.0.0`。
- 重启 HBase 容器。

验证：

```bash
docker exec hbase cat /hbase/conf/hbase-site.xml | grep -E 'hostname|ipc.address'
```

如果输出里还是容器名、内网名或旧 IP，就说明配置没有真正生效。

### 5.2 端口能打开 Web UI，但 Java 访问失败

只开放 `16010` 不够。`16010` 是 Web UI，不是 Java 客户端真实读写数据的唯一端口。

至少检查：

```text
2181, 16000, 16020
```

建议学习阶段直接按本文端口表放行 `2181, 16000, 16010, 16020, 16030`。

判断思路：

- 能打开 `16010` 只能说明 Web UI 可访问。
- 能连 `2181` 才说明 Java 客户端能访问 ZooKeeper。
- 能访问 `16020` 才说明客户端后续读写 RegionServer 有机会成功。

### 5.3 安全组已经放行，但仍然连不上

按下面顺序排查：

```bash
# 1. 云平台安全组是否放行
# 到云控制台检查

# 2. 系统防火墙是否放行
sudo ufw status
sudo iptables -L -n

# 3. 宿主机是否监听
ss -tlnp | grep -E '2181|16000|16010|16020|16030'

# 4. 容器内进程是否存在
docker exec hbase jps
```

### 5.4 低内存导致 HBase 不稳定

1.6Gi 左右的服务器可以作为学习环境，但不适合长期稳定运行 HBase。常见表现：

- 容器启动后退出。
- HMaster 或 RegionServer 不存在。
- Web UI 打不开。
- 日志出现 out of memory。

处理建议：

- 优先升级到 2Gi 以上内存。
- 关闭服务器上不必要的服务。
- 学习环境不要同时跑太多容器。
- 低内存参数只作为临时尝试，不作为生产建议。

如果你看到 HMaster 存在但 RegionServer 不存在，或者 Web UI 里没有可用 RegionServer，Java 写入通常也会失败。这时继续调 Spring Boot 代码意义不大，应该先回到 HBase 容器日志排查。

### 5.5 Windows 本地缺少 winutils

Windows 本地运行 Hadoop/HBase 客户端时可能需要 `winutils.exe`。当前 demo 已内置：

```text
hadoop_home/bin/winutils.exe
hadoop_home/bin/hadoop.dll
```

如果系统没有配置 `HADOOP_HOME`，项目会尝试使用项目目录下的 `hadoop_home`。

---

## 六、单机 Docker 与生产环境的区别

本文方案只适合学习和开发验证。

| 组件 | 本文单机 Docker | 生产环境 |
| --- | --- | --- |
| HMaster | 单实例 | 多节点高可用 |
| RegionServer | 单实例 | 多节点，可水平扩展 |
| ZooKeeper | 容器内置 | 独立 3/5/7 节点集群 |
| 存储 | 容器本地文件系统 | 通常结合 HDFS 或云存储体系 |
| 数据可靠性 | 容器删除可能丢数据 | 多副本、高可靠 |
| 网络安全 | 学习阶段临时开放端口 | 内网访问、权限隔离、审计 |

不要把本文的单机 Docker 方案直接当生产部署方案。它的价值是帮你理解 HBase 的部署结构、客户端连接链路和 Java 访问方式。

---

## 七、后续学习建议

完成本文后，可以按下面路线继续：

1. 理解 HBase 数据模型：表、rowKey、列族、列、版本。
2. 学习 Java 客户端高级能力：批量写、Scan 条件、过滤器。
3. 学习 rowKey 设计：热点问题、散列、时间序列设计。
4. 学习生产部署：独立 ZooKeeper、HDFS、多 RegionServer。
5. 学习监控和排查：JMX、Prometheus、Grafana、慢请求定位。

附赠资料可以这样安排：

- 想看更完整的 Docker 部署、持久化、端口和排查清单：看《HBase-Docker-部署手册.md》。
- 已经跑通过本文，想继续往生产级能力走：看《HBase 学习路线图（Java程序员·生产级视角）.md》。

---

## 八、本文和配套代码的关系

本文讲的是从 0 到 1 跑通 HBase 和 Java 访问链路；[`hbase-springboot-demo`](https://github.com/redants-101/hbase-springboot-demo) 是这条链路的代码化表达。

建议阅读顺序：

1. 先按本文部署 HBase。
2. 用 HBase Shell 创建和查询一条数据。
3. 启动 `hbase-springboot-demo`。
4. 用 REST 接口重复创建表、写入、查询、扫描、删除。
5. 回头看 `HBaseConfig`、`HBaseServiceImpl` 和 `HBaseController`，理解 Java 客户端调用 HBase 的完整过程。

到这里，你就不只是“装好了 HBase”，而是知道了本地 Java 程序到底是怎样通过 ZooKeeper 找到 HBase，再完成真实读写的。
