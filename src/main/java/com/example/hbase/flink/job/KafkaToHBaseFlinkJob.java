package com.example.hbase.flink.job;

import com.example.hbase.flink.config.KafkaToHBasePipelineProperties;
import com.example.hbase.flink.dto.KafkaLogEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Kafka → HBase 实时数据管道的 Flink 作业。
 * <p>
 * 构建并启动 Flink 流处理任务，将 Kafka 指定 Topic 中的消息实时消费并写入 HBase。
 * 作业拓扑结构为：KafkaSource → KafkaToHBaseSinkFunction，
 * 即“读 Kafka → 写 HBase”的两节点流图。
 * </p>
 * <p>
 * 由 {@code KafkaToHBasePipelineService} 在用户触发启动时创建实例，
 * 每次启动会生成新的 {@code runId}，便于日志溯源和多实例隔离。
 * </p>
 */
public class KafkaToHBaseFlinkJob {

    /** 管道配置参数，包含 Kafka 地址、Topic、HBase 表名、并行度等 */
    private final KafkaToHBasePipelineProperties pipelineProperties;

    /** HBase 连接相关配置（zk 地址、端口、超时时间等），由 Service 层从 application.yml 读取后传入 */
    private final Map<String, String> hbaseConfigValues;

    /** 本次运行的唯一标识，用于在日志、指标和 HBase RowKey 中区分不同次启动的数据 */
    private final String runId;

    /**
     * 构造 Flink 作业实例。
     *
     * @param pipelineProperties 管道配置参数（Kafka / HBase / Flink 并行度等）
     * @param hbaseConfigValues  HBase 连接配置键值对
     * @param runId              本次运行的唯一标识 ID
     */
    public KafkaToHBaseFlinkJob(KafkaToHBasePipelineProperties pipelineProperties,
                                Map<String, String> hbaseConfigValues,
                                String runId) {
        this.pipelineProperties = pipelineProperties;
        this.hbaseConfigValues = hbaseConfigValues;
        this.runId = runId;
    }

    /**
     * 构建 Flink 流图并异步启动作业。
     * <p>
     * 流图拓扑：
     * <pre>
     *   KafkaSource (read-kafka-logs)
     *       ↓
     *   KafkaToHBaseSinkFunction (write-logs-to-hbase)
     * </pre>
     * 方法调用后立即返回 {@link JobClient}，作业在后台持续运行，
     * 调用方可通过 {@link JobClient} 监控状态或取消作业。
     * </p>
     *
     * @return Flink 作业客户端，可用于获取作业状态、取消作业等操作
     * @throws Exception 若 Flink 环境初始化或作业提交失败时抛出
     */
    public JobClient start() throws Exception {
        // 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置全局并行度，决定各算子的 Task 槽数量
        env.setParallelism(pipelineProperties.getParallelism());

        // ========================
        // Checkpoint 配置：保障精确一次消费语义
        // ========================
        configureCheckpointing(env);

        // 构建 Kafka Source：配置 Broker 地址、Topic、消费者组、起始偏移量和反序列化器
        KafkaSource<KafkaLogEvent> source = KafkaSource.<KafkaLogEvent>builder()
                .setBootstrapServers(pipelineProperties.getBootstrapServers())  // Kafka Broker 地址
                .setTopics(pipelineProperties.getTopic())                       // 消费的目标 Topic
                .setGroupId(pipelineProperties.getGroupId())                    // 消费者组 ID
                .setStartingOffsets(pipelineProperties.isStartFromEarliest()
                        ? OffsetsInitializer.earliest()     // 从最早消息开始，用于全量回放
                        : OffsetsInitializer.latest())      // 从最新消息开始，用于增量接入
                .setDeserializer(new KafkaRecordToLogEventDeserializer())       // 自定义反序列化器：byte[] → KafkaLogEvent
                .build();

        // 构建流图：Kafka Source → HBase Sink
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka " + pipelineProperties.getTopic())
                .name("Read Kafka logs")           // 算子可读名称，便于在 Flink Web UI 中识别
                .uid("read-kafka-logs")             // 算子唯一 UID，用于 Checkpoint 状态恢复时映射状态
                .addSink(new KafkaToHBaseSinkFunction(pipelineProperties, hbaseConfigValues, runId))
                .name("Write logs to HBase")       // Sink 算子可读名称
                .uid("write-logs-to-hbase");        // Sink 算子唯一 UID

        // 异步提交作业并返回 JobClient，作业名称会显示在 Flink Web UI 的 Job 列表中
        return env.executeAsync("Kafka public-test to HBase");
    }

    /**
     * 启动作业并阻塞等待作业执行完成。
     * <p>
     * 内部调用 {@link #start()} 获取 {@link JobClient}，
     * 再通过 {@link JobClient#getJobExecutionResult()} 返回一个 {@link CompletableFuture}，
     * 调用方可通过 {@code .get()} 阻塞等待作业结束或注册回调。
     * </p>
     *
     * @return 作业执行结果的 {@link CompletableFuture}，作业正常结束或异常终止时完成
     * @throws Exception 若作业启动失败时抛出
     */
    public CompletableFuture<JobExecutionResult> startAndWait() throws Exception {
        return start().getJobExecutionResult();
    }

    /**
     * 配置 Flink Checkpoint 机制。
     * <p>
     * Checkpoint 是 Flink 容错的核心机制，周期性地将算子状态（包括 Kafka 消费进度）
     * 持久化到状态后端。当容器异常重启时，Flink 从最近的 Checkpoint 恢复，
     * 避免消息重复消费或丢失，实现端到端精确一次语义。
     * </p>
     * <p>
     * 配合 KafkaToHBaseSinkFunction 的 RowKey 设计（topic-partition-offset），
     * 即使 Checkpoint 恢复后重复写入，相同 RowKey 的 Put 操作会覆盖而非追加，天然幂等。
     * </p>
     *
     * @param env Flink 流执行环境
     */
    private void configureCheckpointing(StreamExecutionEnvironment env) {
        long intervalMs = pipelineProperties.getCheckpointIntervalMs();

        // checkpointIntervalMs = 0 表示禁用 Checkpoint
        if (intervalMs <= 0) {
            return;
        }

        CheckpointConfig cpConfig = env.getCheckpointConfig();

        // 设置 Checkpoint 模式为 EXACTLY_ONCE（精确一次），这是默认值但显式声明更清晰
        cpConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置 Checkpoint 间隔：每隔 intervalMs 毫秒触发一次 Checkpoint
        cpConfig.setCheckpointInterval(intervalMs);

        // 设置两次 Checkpoint 之间的最小暂停时间，避免前一次尚未完成又触发新的
        cpConfig.setMinPauseBetweenCheckpoints(pipelineProperties.getMinPauseBetweenCheckpointsMs());

        // 设置 Checkpoint 超时时间，超时未完成则视为失败
        cpConfig.setCheckpointTimeout(pipelineProperties.getCheckpointTimeoutMs());

        // 作业取消时保留 Checkpoint 状态，便于手动恢复
        cpConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 允许 Checkpoint 失败时不立即停止作业，给一定的容忍次数
        cpConfig.setTolerableCheckpointFailureNumber(3);

        // 设置固定延迟重启策略：最多重试 3 次，每次间隔 10 秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)
        ));
    }
}
