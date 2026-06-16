## 一、Docker 环境准备与镜像拉取

### 1.1 安装 Docker

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install -y docker.io
sudo systemctl enable docker
sudo systemctl start docker

# 验证安装
docker --version
```

### 1.2 拉取 HBase 镜像

```bash
# 拉取官方 HBase 镜像
docker pull harisekhon/hbase:latest

# 验证镜像
docker images | grep hbase
```

---

## 二、容器创建与端口映射

### 2.1 创建 HBase 容器

```bash
docker run -d \
  --name hbase \
  --hostname hbase \
  -p 2181:2181 \
  -p 16000:16000 \
  -p 16010:16010 \
  -p 16020:16020 \
  -p 16030:16030 \
  -p 9090:9090 \
  -p 9095:9095 \
  harisekhon/hbase:latest
```

### 2.2 端口映射说明

| 容器端口 | 宿主机端口 | 用途 |
|----------|------------|------|
| 2181 | 2181 | ZooKeeper 客户端连接 |
| 16000 | 16000 | Master RPC 通信 |
| 16010 | 16010 | Master Web UI |
| 16020 | 16020 | RegionServer RPC |
| 16030 | 16030 | RegionServer Web UI |
| 9090 | 9090 | Thrift Server |
| 9095 | 9095 | REST Server |

---

## 三、hbase-site.xml 配置修改

### 3.1 进入容器修改配置

```bash
# 进入容器
docker exec -it hbase bash

# 编辑 hbase-site.xml
vi /hbase/conf/hbase-site.xml
```

### 3.2 关键配置项

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

> **⚠️ 关键**：`hbase.master.hostname` 和 `hbase.regionserver.hostname` 必须设置为服务器的实际公网 IP（`140.143.201.112`），否则客户端通过 ZooKeeper 获取到容器内部 hostname 将无法连接。

### 3.3 重启容器使配置生效

```bash
docker restart hbase
```

---

## 四、防火墙配置

### 4.1 端口说明

| 端口 | 用途 | 协议 | 说明 |
|------|------|------|------|
| 2181 | ZooKeeper 客户端连接 | TCP | Java 客户端首先连接此端口获取集群信息 |
| 16000 | Master RPC 通信 | TCP | Master 与客户端/RegionServer 之间的 RPC 通信端口 |
| 16010 | Master Web UI | TCP | HBase Master 的 Web 管理界面 |
| 16020 | RegionServer RPC | TCP | RegionServer 与客户端/Master 之间的 RPC 通信 |
| 16030 | RegionServer Web UI | TCP | RegionServer 的 Web 信息界面 |
| 9090 | Thrift Server | TCP | Thrift API 访问端口（可选） |
| 9095 | REST Server | TCP | REST API 访问端口（可选） |

### 4.2 配置腾讯云 Lighthouse 防火墙

> **⚠️ 关键提醒**：云平台安全组/防火墙是端口访问问题的第一优先级排查点（占 90% 的问题），务必优先配置！

1. 登录腾讯云控制台 → 轻量应用服务器 → 实例列表
2. 点击实例 ID `lhins-23p7b71i` → **防火墙** 标签页
3. 点击 **添加规则**，依次添加以下端口：

```
端口列表：2181, 16000, 16010, 16020, 16030, 9090, 9095
协议：TCP
策略：允许
备注：HBase 服务端口
```

4. 添加完成后，防火墙规则应包含上述所有端口

### 4.3 验证防火墙规则

```bash
# 查看当前防火墙规则
sudo ufw status

# 如果启用了 ufw，需要放行端口
sudo ufw allow 2181/tcp
sudo ufw allow 16000/tcp
sudo ufw allow 16010/tcp
sudo ufw allow 16020/tcp
sudo ufw allow 16030/tcp
sudo ufw allow 9090/tcp
sudo ufw allow 9095/tcp
```

---

## 五、云防火墙安全组放行

### 5.1 腾讯云 Lighthouse 防火墙规则

确保以下端口已在云防火墙中放行：

| 端口 | 用途 |
|------|------|
| 2181 | ZooKeeper |
| 16000 | Master RPC |
| 16010 | Master Web UI |
| 16020 | RegionServer RPC |
| 16030 | RegionServer Web UI |
| 9090 | Thrift Server |
| 9095 | REST Server |

### 5.2 验证云防火墙

```bash
# 从外部测试端口连通性（在本地执行）
telnet 140.143.201.112 2181
curl -s http://140.143.201.112:16010/master-status | head -5
```

---

## 六、验证测试

### 6.1 Web UI 验证

```bash
# Master Web UI
curl -s http://140.143.201.112:16010/master-status | grep -i hbase

# RegionServer Web UI
curl -s http://140.143.201.112:16030/rs-status | grep -i regionserver
```

### 6.2 Java API 验证

```java
// 示例：通过 Java 客户端连接 HBase
Configuration config = HBaseConfiguration.create();
config.set("hbase.zookeeper.quorum", "140.143.201.112");
config.set("hbase.zookeeper.property.clientPort", "2181");

Connection connection = ConnectionFactory.createConnection(config);
Admin admin = connection.getAdmin();

// 列出所有表
TableName[] tables = admin.listTableNames();
for (TableName table : tables) {
    System.out.println(table.getNameAsString());
}

admin.close();
connection.close();
```

### 6.3 HBase Shell 验证

```bash
# 进入容器执行 HBase Shell
docker exec -it hbase hbase shell

# 在 Shell 中执行
list
create 'test_table', 'cf'
put 'test_table', 'row1', 'cf:col1', 'value1'
scan 'test_table'
```

---

## 七、容器数据持久化与开机自启

### 7.1 数据持久化（-v 挂载）

```bash
# 创建宿主机数据目录
mkdir -p /data/hbase

# 重新创建容器并挂载数据卷
docker rm -f hbase
docker run -d \
  --name hbase \
  --hostname hbase \
  -v /data/hbase:/hbase-data \
  -p 2181:2181 \
  -p 16000:16000 \
  -p 16010:16010 \
  -p 16020:16020 \
  -p 16030:16030 \
  -p 9090:9090 \
  -p 9095:9095 \
  harisekhon/hbase:latest
```

### 7.2 宿主机开机自启

```bash
# 编辑 rc.local
sudo vi /etc/rc.local

# 添加以下内容（在 exit 0 之前）
docker start hbase

# 赋予执行权限
sudo chmod +x /etc/rc.local
```

---

## 八、容器重建三步流程

### 8.1 标准重建流程

```bash
# 步骤1：停止并删除旧容器
docker stop hbase
docker rm hbase

# 步骤2：重新创建容器（保留数据卷）
docker run -d \
  --name hbase \
  --hostname hbase \
  -v /data/hbase:/hbase-data \
  -p 2181:2181 \
  -p 16000:16000 \
  -p 16010:16010 \
  -p 16020:16020 \
  -p 16030:16030 \
  -p 9090:9090 \
  -p 9095:9095 \
  harisekhon/hbase:latest

# 步骤3：验证服务状态
docker logs -f hbase
```

---

## 九、常见问题排查

### 9.1 端口无法访问排查

**排查优先级**：云平台安全组 → 系统防火墙 → 端口监听

```bash
# 1. 检查云防火墙规则（腾讯云控制台）
# 2. 检查系统防火墙
sudo ufw status
sudo iptables -L -n

# 3. 检查端口监听
ss -tlnp | grep -E '2181|16000|16010|16020|16030|9090|9095'
```

### 9.2 UnknownHostException 排查

```bash
# 检查 hbase-site.xml 中的 hostname 配置
docker exec hbase cat /hbase/conf/hbase-site.xml | grep hostname

# 确保 hostname 设置为实际 IP 而非容器内部名称
```

### 9.3 DNS 解析警告（WARN 可忽略）

```
WARN: Unable to resolve hostname "xxx"
```

> 此警告通常可忽略，不影响 HBase 正常运行。

### 9.4 lo 接口 IP 丢失问题

```bash
# 检查 lo 接口
ip addr show lo

# 如果 lo 接口没有 127.0.0.1，重新添加
sudo ip addr add 127.0.0.1/8 dev lo
```

### 9.5 FailedServerException 排查

```bash
# 查看 Master 日志
docker logs hbase 2>&1 | grep -i error

# 检查 RegionServer 状态
curl -s http://140.143.201.112:16010/master-status | grep -i regionserver
```

### 9.6 端口未监听排查

```bash
# 检查容器内端口监听
docker exec hbase ss -tlnp

# 检查 HBase 进程
docker exec hbase jps
```

---

## 十、6 个关键踩坑记录

1. **hostname 必须用实际 IP**：容器内 hostname 设为实际公网 IP，否则客户端无法连接
2. **云防火墙是第一优先级**：90% 的端口访问问题都是云平台安全组未放行
3. **16000 端口容易遗漏**：Master RPC 端口，Java 客户端 DDL 操作必需
4. **数据持久化必须挂载卷**：否则容器删除后数据丢失
5. **ipc.address 与 hostname 职责分离**：ipc.address 控制绑定地址（0.0.0.0），hostname 控制对外宣告地址（实际 IP）
6. **重启后需等待 HBase 完全启动**：Master 和 RegionServer 启动需要约 30-60 秒

---

## 十一、运维常用命令

```bash
# 查看容器状态
docker ps -a | grep hbase

# 查看容器日志
docker logs -f hbase

# 进入容器
docker exec -it hbase bash

# 进入 HBase Shell
docker exec -it hbase hbase shell

# 重启容器
docker restart hbase

# 检查端口监听
ss -tlnp | grep -E '2181|16000|16010|16020|16030|9090|9095'

# 检查 HBase 进程
docker exec hbase jps

# 备份数据
tar -czf hbase-backup-$(date +%Y%m%d).tar.gz /data/hbase/
```

---

## 十二、三层持久化保障

| 层级 | 保障措施 | 说明 |
|------|----------|------|
| 数据层 | `-v /data/hbase:/hbase-data` | 容器数据持久化到宿主机 |
| 服务层 | `docker restart hbase` + rc.local | 容器开机自启 |
| 网络层 | 云防火墙 + ufw 双重放行 | 确保端口可访问 |

### 防火墙排查优先级

> **云平台安全组（90%）> 系统防火墙（8%）> 端口监听（2%）**

### ipc.address 与 hostname 职责分离

| 配置项 | 作用 | 推荐值 |
|--------|------|--------|
| `hbase.master.ipc.address` | 控制 Master 绑定的网络接口 | `0.0.0.0`（监听所有接口） |
| `hbase.master.hostname` | 控制 Master 向 ZooKeeper 注册的地址 | 实际公网 IP |
| `hbase.regionserver.ipc.address` | 控制 RegionServer 绑定的网络接口 | `0.0.0.0`（监听所有接口） |
| `hbase.regionserver.hostname` | 控制 RegionServer 向 Master 注册的地址 | 实际公网 IP |

> **记忆口诀**：ipc 管绑定（对内），hostname 管宣告（对外）