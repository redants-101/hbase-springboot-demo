package com.example.hbase.flink.dto;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * Kafka 日志事件的数据传输对象（DTO）。
 * <p>
 * 封装从 Kafka 消费到的单条消息的元数据与消息内容，
 * 在 Flink 作业中作为流处理的中间载体，最终写入 HBase。
 * 实现 {@link Serializable} 以支持 Flink 的分布式序列化传输。
 * </p>
 */
@Setter
@Getter
public class KafkaLogEvent implements Serializable {

    /** 消息来源的 Kafka Topic 名称 */
    private String topic;

    /** 消息所在的 Kafka 分区编号 */
    private int partition;

    /** 消息在分区内的偏移量，用于唯一标识一条消息 */
    private long offset;

    /** Kafka 为消息打上的时间戳（毫秒），可用于事件时间语义 */
    private long kafkaTimestamp;

    /** 消息体的原始字符串内容 */
    private String value;

    /** 无参构造函数，供 Flink 反序列化框架使用 */
    public KafkaLogEvent() {
    }

    /**
     * 全参构造函数，用于将 Kafka 消费到的消息封装为事件对象。
     *
     * @param topic          消息来源的 Topic 名称
     * @param partition      消息所在的分区编号
     * @param offset         消息在分区内的偏移量
     * @param kafkaTimestamp Kafka 消息时间戳（毫秒）
     * @param value          消息体的原始字符串内容
     */
    public KafkaLogEvent(String topic, int partition, long offset, long kafkaTimestamp, String value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.kafkaTimestamp = kafkaTimestamp;
        this.value = value;
    }

}
