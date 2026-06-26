package com.example.hbase.flink.dto;

import com.example.hbase.flink.config.KafkaToHBasePipelineProperties;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Kafka → HBase 数据管道的运行时状态快照。
 * <p>
 * 聚合了 Flink 作业的运行状态、消息处理统计、最近写入预览等信息，
 * 通过 REST 接口返回给前端，用于实时监控与运维排查。
 * </p>
 */
@Setter
@Getter
public class KafkaToHBasePipelineStatus {

    /** 管道是否正在运行中 */
    private boolean running;

    /** Flink 作业的当前状态，如 RUNNING、CANCELED、FAILED 等 */
    private String state;

    /** 管道状态的附加描述信息，如启动成功提示或失败原因摘要 */
    private String message;

    /** Flink 作业的唯一标识 ID，由 JobManager 分配 */
    private String jobId;

    /** 管道启动时间 */
    private LocalDateTime startedAt;

    /** 管道停止时间；若仍在运行则为 null */
    private LocalDateTime stoppedAt;

    /** 从 Kafka 累计消费到的消息条数 */
    private long receivedCount;

    /** 成功写入 HBase 的消息条数 */
    private long writtenCount;

    /** 写入 HBase 失败的消息条数 */
    private long failedCount;

    /** 最近一次写入失败时的错误信息；无错误时为 null */
    private String lastError;

    /** 当前生效的管道配置参数，便于运维侧确认运行时配置是否正确 */
    private KafkaToHBasePipelineProperties config;

    /** 最近成功写入 HBase 的数据行预览列表，用于前端实时展示 */
    private List<KafkaToHBasePreviewRow> recentRows = new ArrayList<>();

}
