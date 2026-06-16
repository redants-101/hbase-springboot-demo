### 阶段一：巩固基础与原理深挖（1-2周）

**目标**：彻底搞懂HBase的数据模型、核心读写流程、以及为什么它适合海量数据。

| 主题     | 关键任务                                                     | 学习资源                                                     |
| :------- | :----------------------------------------------------------- | :----------------------------------------------------------- |
| 数据模型 | 理解Table、RowKey、Column Family、Qualifier、Timestamp、Version。能够设计简单的RowKey。 | 官方文档《Apache HBase Reference Guide》的“Data Model”章节。 |
| 读写流程 | 搞清楚：写路径（WAL→MemStore→HFile），读路径（BlockCache→MemStore→HFile）。理解刷写（flush）和合并（compaction）。 | 博客《HBase读写流程详解》，画出自绘流程图。                  |
| 核心特性 | 强一致性（行级）、自动分区（Region split）、LSM树原理。      | Google Bigtable论文。                                        |
| 实践     | 在你现有的Docker HBase中，创建多列族表，插入不同版本的数据，用`scan`的`{VERSIONS => 3}`参数查看多版本。 | HBase Shell练习。                                            |

**验收标准**：能用一句话向别人解释清楚“HBase为什么适合写多读少的场景”。

------

### 阶段二：开发进阶与API精通（2-3周）

**目标**：熟练使用Java API完成复杂操作，掌握过滤器、协处理器、批量操作等高级特性。

| 主题                    | 关键任务                                                     | 实践项目                                           |
| :---------------------- | :----------------------------------------------------------- | :------------------------------------------------- |
| 高级API                 | 批量`put`/`delete`/`get`，`increment`（计数器），`append`（追加）。 | 写一个模拟“用户行为日志入库”的程序，使用批量写入。 |
| 过滤器（Filter）        | `RowFilter`、`ColumnPrefixFilter`、`SingleColumnValueFilter`、`FilterList`。 | 实现一个按时间范围查询+正则匹配行键的扫描。        |
| 协处理器（Coprocessor） | 理解Observer和Endpoint的区别。编写一个简单的RegionObserver，在插入数据时自动记录审计日志。 | 部署协处理器到你的单机环境（注意性能影响）。       |
| 异步客户端              | 使用`AsyncConnection`和`CompletableFuture`编写非阻塞代码。   | 对比同步和异步的性能差异。                         |

**验收标准**：能写出一个支持分页查询的工具类，利用`PageFilter`和行键偏移实现。

------

### 阶段三：集群部署与高可用（3-4周）

**目标**：搭建全分布式HBase集群（至少3节点），集成独立ZooKeeper和HDFS，并配置高可用。

| 主题            | 关键任务                                                     | 环境与工具                                |
| :-------------- | :----------------------------------------------------------- | :---------------------------------------- |
| 环境准备        | 用虚拟机（或云服务器）准备3台Linux机器（2核4G以上）。安装JDK、配置SSH免密。 | VMware/VirtualBox，或阿里云按量付费实例。 |
| HDFS部署        | 部署Hadoop 3.x集群：1个NameNode+2个DataNode，配置高可用（可选）。 | Hadoop官方文档。                          |
| ZooKeeper部署   | 部署3节点ZooKeeper集群，确保与HBase独立。                    | ZK官方文档。                              |
| HBase分布式部署 | 配置`hbase-site.xml`：`hbase.rootdir`指向HDFS、`hbase.zookeeper.quorum`指向ZK集群。启动HMaster和RegionServer。 | 《HBase集群搭建》博客。                   |
| 高可用验证      | 手动kill Active HMaster，观察Standby是否能接管。kill一个RegionServer，观察其上的Region是否迁移到其他节点。 | 使用`hbck`工具检查。                      |

**验收标准**：成功搭建一个3节点的HBase集群，并能通过任意节点的HMaster UI看到所有RegionServer在线。

------

### 阶段四：性能调优与运维（3-4周）

**目标**：掌握生产环境下的参数调优、监控告警、备份恢复和滚动升级。

| 主题     | 关键任务                                                     | 常用指标/工具                                 |
| :------- | :----------------------------------------------------------- | :-------------------------------------------- |
| JVM调优  | 为RegionServer配置G1GC，调整内存比例（MemStore vs BlockCache）。 | `hbase_region_server_gc_*`指标。              |
| 读写调优 | 调整`hbase.regionserver.handler.count`、`hbase.client.write.buffer`、`hbase.hregion.memstore.flush.size`。 | 使用`hbase pe`（PerformanceEvaluation）压测。 |
| 监控告警 | 集成Prometheus + Grafana，导入HBase的JMX exporter。配置告警规则（如RegionServer宕机、HDFS使用率>80%）。 | HBase JMX metrics列表。                       |
| 数据备份 | 使用`snapshot`快照备份表，并测试恢复到新表。                 | `clone_snapshot`、`restore_snapshot`命令。    |
| 滚动升级 | 在不停止服务的前提下，逐个升级RegionServer和HMaster。        | 官方《Rolling Upgrade》文档。                 |

**验收标准**：能根据监控曲线判断是否存在读写热点，并修改预分区策略或RowKey设计来打散负载。

------

### 阶段五：生态集成与生产实战（2-3周）

**目标**：将HBase与Phoenix、Spark、Kafka等组件结合，解决真实业务场景。

| 主题                   | 关键任务                                                  | 项目示例                                               |
| :--------------------- | :-------------------------------------------------------- | :----------------------------------------------------- |
| **Phoenix**（SQL层）   | 在HBase上映射表，使用标准SQL进行查询和二级索引。          | 创建用户订单表，用SQL查询“最近7天订单金额Top 10用户”。 |
| **Spark on HBase**     | 使用Spark读取HBase表并做分析，结果写回HBase。             | 计算每小时PV/UV，结果存入HBase。                       |
| **Kafka + HBase**      | 使用Kafka Connect或Java程序消费Kafka数据，批量写入HBase。 | 实时日志入库，进行用户画像标签更新。                   |
| **HBase与Spring Boot** | 封装HBase DAO层，提供REST API给业务系统。                 | 开发一个简单的“用户积分系统”微服务。                   |
| **生产巡检**           | 学习使用`hbase hbck`、`hbase hbck -fix`、`canary`工具。   | 模拟元数据不一致，尝试修复。                           |

**验收标准**：完成一个端到端的小项目：Kafka → HBase → Phoenix/Spark 查询 → Web展示。

------

## 📚 推荐的读书与认证路径

| 类型         | 资源                                                         |
| :----------- | :----------------------------------------------------------- |
| **必读书籍** | 《HBase权威指南》、《HBase应用架构》、《Hadoop权威指南》（HDFS章节） |
| **文档**     | Apache HBase官方Reference Guide（尤其是“Book”部分）、Apache Phoenix文档 |
| **视频课程** | 慕课网《HBase从入门到实战》、Cloudera 的 HBase 培训（免费）  |
| **认证**     | Cloudera CCA-500（HBase组件部分）、阿里云ACP大数据认证       |

------

## 🛠️ 你的专属学习计划（建议）

根据你当前水平（已能单机部署+Java API），我为你建议一个**12周**的学习计划，每天投入1-2小时：

| 周次      | 阶段      | 重点任务                                                     |
| :-------- | :-------- | :----------------------------------------------------------- |
| 第1-2周   | 阶段一    | 深入原理，画出读写流程图，总结RowKey设计原则。               |
| 第3-4周   | 阶段二    | 用Java API实现一个“车辆GPS轨迹入库”小项目，用到过滤器和批量操作。 |
| 第5-6周   | 阶段三    | 用虚拟机搭建3节点集群，验证HA。写出详细的部署脚本。          |
| 第7-8周   | 阶段四    | 用`hbase pe`压测并调优参数，搭建Prometheus+Grafana监控。     |
| 第9-10周  | 阶段五    | 集成Phoenix和Spark，完成一个实时分析项目。                   |
| 第11-12周 | 复习+分享 | 整理成博客或PPT，向团队分享你的学习成果。                    |

------

## ✅ 检查清单

当你完成以下所有项，说明你已经具备了生产级HBase能力：

- 能在物理或虚拟机上独立部署HDFS、ZK、HBase全分布式集群。
- 熟悉至少10个核心配置参数的含义和调优场景。
- 能使用Java API+过滤器实现复杂的查询逻辑。
- 会使用Phoenix进行二级索引和SQL查询。
- 会使用Spark读取HBase并进行聚合分析。
- 能通过Prometheus+Grafana监控集群健康状况。
- 懂得如何安全地进行数据迁移、备份和恢复。
- 能够设计高吞吐的RowKey，避免热点。

------

你已经在正确的轨道上。记住：**HBase是工具，不是目标**。更重要的是理解它背后的分布式系统思想（LSM、最终一致性、CAP取舍）。如果学习过程中遇到任何问题，随时可以回来问我。祝你早日成为HBase生产专家！