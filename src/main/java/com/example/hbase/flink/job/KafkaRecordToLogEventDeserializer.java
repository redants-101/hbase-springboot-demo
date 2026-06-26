package com.example.hbase.flink.job;

import com.example.hbase.flink.dto.KafkaLogEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

/**
 * Kafka 原始字节消息 → {@link KafkaLogEvent} 的反序列化器。
 * <p>
 * 实现 Flink 的 {@link KafkaRecordDeserializationSchema} 接口，
 * 负责将 Kafka Source 读到的原始 {@code byte[]} 消息解析为业务层的
 * {@link KafkaLogEvent} 对象，同时保留 Kafka 的元数据（topic、partition、
 * offset、timestamp），供下游算子做溯源、去重或事件时间窗口处理。
 * </p>
 * <p>
 * 该反序列化器在 {@code KafkaToHBaseFlinkJob} 中注册到 Kafka Source，
 * 是数据管道的第一个处理环节。
 * </p>
 */
public class KafkaRecordToLogEventDeserializer implements KafkaRecordDeserializationSchema<KafkaLogEvent> {

    /**
     * 将 Kafka 原始消息反序列化为 {@link KafkaLogEvent} 并发送到下游算子。
     *
     * @param record Kafka 原始字节消息，包含 topic、partition、offset、timestamp 等元数据
     *               以及 byte[] 类型的消息体；消息体可能为 null（如 Kafka tombstone 消息）
     * @param out    Flink 输出收集器，调用 {@code collect} 将解析后的事件发送到下游
     */
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaLogEvent> out) {
        // 将 byte[] 消息体转为 UTF-8 字符串；若消息体为 null（tombstone 消息），则使用空字符串兜底，避免下游空指针异常
        String value = record.value() == null ? "" : new String(record.value(), StandardCharsets.UTF_8);

        // 封装为 KafkaLogEvent，将 Kafka 元数据与消息内容一并传递给下游算子
        out.collect(new KafkaLogEvent(
                record.topic(),      // 消息来源 Topic
                record.partition(),  // 所在分区编号
                record.offset(),     // 分区内偏移量，可用于消息溯源与去重
                record.timestamp(),  // Kafka 消息时间戳（毫秒），可用于事件时间窗口
                value                // 解析后的消息体字符串
        ));
    }

    /**
     * 返回本反序列化器产出数据的类型信息，供 Flink 类型系统进行序列化与状态管理。
     *
     * @return {@link KafkaLogEvent} 的 {@link TypeInformation} 描述
     */
    @Override
    public TypeInformation<KafkaLogEvent> getProducedType() {
        return TypeInformation.of(KafkaLogEvent.class);
    }
}
