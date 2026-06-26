package com.example.hbase.flink.dto;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Kafka → HBase 管道写入预览行的数据传输对象（DTO）。
 * <p>
 * 记录单条成功写入 HBase 的数据行详情，包括行键、原始消息、
 * Kafka 来源元数据、写入时间以及解析后的列值，
 * 用于前端预览页面展示最近写入的实时数据。
 * </p>
 */
@Setter
@Getter
public class KafkaToHBasePreviewRow {

    /** HBase 行键，由消息元数据组合生成，用于在 HBase 中唯一定位一行 */
    private String rowKey;

    /** Kafka 消息的原始字符串内容，未经解析的原始报文 */
    private String raw;

    /** 消息来源的 Kafka Topic 名称 */
    private String topic;

    /** 消息所在的 Kafka 分区编号 */
    private int partition;

    /** 消息在分区内的偏移量，可与 topic+partition 联合追溯原始消息 */
    private long offset;

    /** 数据写入 HBase 的时间戳 */
    private LocalDateTime writtenAt;

    /** 解析后的列名→列值映射，使用 LinkedHashMap 保持列的插入顺序 */
    private Map<String, String> parsedColumns = new LinkedHashMap<>();

}
