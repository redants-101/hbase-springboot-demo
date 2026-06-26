package com.example.hbase.flink.service;

import com.example.hbase.flink.dto.KafkaToHBasePreviewRow;

/**
 * Kafka → HBase 数据管道的运行时指标收集接口。
 * <p>
 * 定义管道在运行期间需要上报的三类事件回调，
 * 由 {@code KafkaToHBaseSinkFunction} 在消费与写入过程中调用，
 * 实现类负责维护统计计数、错误信息和最近写入预览数据，
 * 并通过 REST 接口暴露给前端，用于实时监控面板展示。
 * </p>
 * <p>
 * 实例通过 {@link KafkaToHBasePipelineMetricsRegistry} 按 {@code runId} 统一注册和获取，
 * 保证 Flink TaskManager 与 Spring 主进程之间共享同一指标对象。
 * </p>
 *
 * @see com.example.hbase.flink.job.KafkaToHBaseSinkFunction
 * @see KafkaToHBasePipelineMetricsRegistry
 */
public interface KafkaToHBasePipelineMetrics {

    /**
     * 从 Kafka 成功消费到一条消息时调用。
     * <p>
     * 实现类应在此方法中将接收计数器 +1，
     * 用于前端展示累计消费消息条数。
     * </p>
     */
    void onReceived();

    /**
     * 一条消息成功写入 HBase 后调用。
     * <p>
     * 实现类应在此方法中将写入成功计数器 +1，
     * 并将传入的预览行追加到最近写入列表中，供前端实时展示。
     * </p>
     *
     * @param row 成功写入的数据行预览对象，包含 RowKey、原始消息、Kafka 元数据和解析后的列
     */
    void onWritten(KafkaToHBasePreviewRow row);

    /**
     * 一条消息写入 HBase 失败时调用。
     * <p>
     * 实现类应在此方法中将失败计数器 +1，
     * 并记录异常信息（如 {@code e.getMessage()}），供前端展示最近一次错误详情。
     * </p>
     *
     * @param e 写入过程中抛出的异常，包含失败原因
     */
    void onFailed(Exception e);
}
