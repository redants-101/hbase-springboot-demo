# HBase Spring Boot 示例

这是一个基于 Spring Boot 2.7 的 HBase 示例项目，对外提供简单的 REST 接口，用于创建表、写入单元格、读取单元格、读取整行、扫描表和删除行。

## 环境要求

- JDK 17
- Maven 3.8+
- 可访问的 HBase 2.x 集群和 ZooKeeper
- Windows 环境如果没有配置 `HADOOP_HOME`，项目会默认使用内置的 `hadoop_home/bin/winutils.exe` 和 `hadoop.dll`。

## 配置说明

HBase 连接配置支持通过环境变量覆盖，未配置时使用默认值：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `HBASE_ZK_QUORUM` | `localhost` | ZooKeeper 地址，多个地址用逗号分隔 |
| `HBASE_ZK_PORT` | `2181` | ZooKeeper 客户端端口 |
| `HBASE_ZNODE_PARENT` | `/hbase` | HBase 在 ZooKeeper 中的根节点 |
| `HBASE_CLIENT_RETRIES` | `3` | HBase 客户端重试次数 |
| `HBASE_OPERATION_TIMEOUT` | `10000` | 操作超时时间，单位毫秒 |
| `HBASE_RPC_TIMEOUT` | `60000` | RPC 超时时间，单位毫秒 |
| `HBASE_SCANNER_CACHING` | `100` | 扫描缓存条数 |

示例：

```powershell
$env:HBASE_ZK_QUORUM="192.168.1.10"
$env:HBASE_ZK_PORT="2181"
mvn spring-boot:run
```

## 构建和启动

```powershell
mvn clean package
java -jar target/hbase-springboot-demo-1.0.0.jar
```

服务默认启动在 `8080` 端口。

## 接口示例

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

## 响应格式

成功响应的 `code` 为 `0`：

```json
{
  "code": 0,
  "message": "success",
  "data": "SpringBootHBase"
}
```

参数校验失败或业务异常会返回对应的 HTTP 状态码，并使用非零 `code`。

## 测试说明

`HBaseServiceIT` 是集成测试，需要真实可访问的 HBase 集群。该测试类使用 `IT` 后缀命名，因此不会被 Maven Surefire 默认的 `mvn test` 执行。

## 备注

- 当前项目默认不内置 HBase 服务，需要连接已有的 HBase 和 ZooKeeper 环境。
- 本地开发时建议通过环境变量配置 HBase 地址，避免把个人或服务器地址写入代码仓库。
- Windows 环境下如果已经配置了 `HADOOP_HOME`，会优先使用系统环境变量；否则使用项目内置的 `hadoop_home`。
