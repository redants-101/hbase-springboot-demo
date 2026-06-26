package com.example.hbase.flink.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.Serializable;

/**
 * Kafka → HBase 实时数据管道的配置属性类。
 * <p>
 * 通过 {@code pipeline.kafka-to-hbase} 前缀从 application.yml 中自动绑定配置项，
 * 涵盖 Kafka 消费端、HBase 写入端以及 Flink 作业运行参数。
 * </p>
 */
@Data
@Component
@ConfigurationProperties(prefix = "pipeline.kafka-to-hbase")
public class KafkaToHBasePipelineProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Kafka Broker 连接地址，多个地址以逗号分隔 */
    private String bootstrapServers;

    /** 消费的 Kafka Topic 名称 */
    private String topic;

    /** Kafka 消费者组 ID，用于标识一组协同消费的消费者实例 */
    private String groupId;

    /** 是否从 Topic 最早的消息开始消费；设为 false 则从最新消息开始 */
    private boolean startFromEarliest = true;

    /** 数据写入的 HBase 目标表名 */
    private String hbaseTable;

    /** HBase 目标表的列族名称 */
    private String columnFamily = "info";

    /** Flink 作业并行度，决定同时运行的 Task 数量 */
    private int parallelism = 1;

    /** 若 HBase 中不存在目标表，是否自动创建 */
    private boolean autoCreateTable = true;

    /** 预览页面保留的最近记录条数，用于在 Web UI 中展示实时数据 */
    private int previewSize = 20;

    /**
     * Flink Checkpoint 间隔时间（毫秒）。
     * <p>
     * 设为 0 表示禁用 Checkpoint。
     * 开启 Checkpoint 后，Flink 会周期性地将 Kafka 消费进度持久化，
     * 容器异常重启时从最近的 Checkpoint 恢复，避免消息重复消费或丢失。
     * 推荐值：30000（30秒）~ 60000（60秒）。
     * </p>
     */
    private long checkpointIntervalMs = 30000;

    /**
     * 两次 Checkpoint 之间的最小暂停时间（毫秒）。
     * <p>
     * 防止前一次 Checkpoint 尚未完成时又触发新的 Checkpoint，造成资源浪费。
     * 一般设置为 checkpointIntervalMs 的一半或相同值。
     * </p>
     */
    private long minPauseBetweenCheckpointsMs = 15000;

    /**
     * Checkpoint 超时时间（毫秒）。
     * <p>
     * 若一次 Checkpoint 在超时内未完成，则视为失败，Flink 将触发重启策略。
     * 默认 10 分钟（600000 毫秒）。
     * </p>
     */
    private long checkpointTimeoutMs = 600000;

}
