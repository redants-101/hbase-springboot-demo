package com.example.hbase.flink.service;

import com.example.hbase.exception.BusinessException;
import com.example.hbase.flink.config.KafkaToHBasePipelineProperties;
import com.example.hbase.flink.dto.KafkaToHBasePipelineStatus;
import com.example.hbase.flink.dto.KafkaToHBasePreviewRow;
import com.example.hbase.flink.job.KafkaToHBaseFlinkJob;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka → HBase 实时数据管道的核心管理服务。
 * <p>
 * 负责管道的完整生命周期管理（启动、停止、状态查询），
 * 同时实现 {@link KafkaToHBasePipelineMetrics} 接口，
 * 作为 Flink Sink 算子的指标上报目标，维护接收/写入/失败计数和最近写入预览数据。
 * </p>
 * <p>
 * <b>架构设计：</b>
 * <ul>
 *   <li>通过单线程 {@link ExecutorService} 异步执行 Flink 作业，避免阻塞 Spring 主线程</li>
 *   <li>在启动时生成唯一 {@code runId}，通过 {@link KafkaToHBasePipelineMetricsRegistry}
 *       将自身注册为指标实例，Flink Sink 通过相同 runId 获取并上报指标</li>
 *   <li>使用 {@code lifecycleMonitor} 保护启停操作的原子性，{@code previewMonitor} 保护预览列表的并发读写</li>
 *   <li>通过 {@code @PreDestroy} 确保 Spring 容器关闭时优雅停止作业并释放资源</li>
 * </ul>
 * </p>
 *
 * @see KafkaToHBasePipelineMetrics
 * @see KafkaToHBasePipelineMetricsRegistry
 * @see com.example.hbase.flink.job.KafkaToHBaseFlinkJob
 */
@Service
public class KafkaToHBasePipelineService implements KafkaToHBasePipelineMetrics {
    private static final Logger log = LoggerFactory.getLogger(KafkaToHBasePipelineService.class);

    // ==================== Spring 注入的配置依赖 ====================

    /** 管道配置参数（Kafka 地址、Topic、HBase 表名、并行度等） */
    private final KafkaToHBasePipelineProperties properties;
    /** Spring 容器管理的 HBase 配置对象，包含 zk 地址、端口、超时等连接参数 */
    private final org.apache.hadoop.conf.Configuration hbaseConfiguration;

    // ==================== 线程与并发控制 ====================

    /**
     * 单线程线程池，专门运行 Flink 作业。
     * <p>
     * 使用守护线程（daemon），不阻止 JVM 退出；
     * 线程名称设为 {@code kafka-to-hbase-flink-job}，便于线程转储时识别。
     * </p>
     */
    private final ExecutorService executorService = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r, "kafka-to-hbase-flink-job");
        thread.setDaemon(true);
        return thread;
    });
    /** 管道启停操作的互斥锁，保证 start/stop 不会并发执行 */
    private final Object lifecycleMonitor = new Object();
    /** 预览列表的读写锁，保护 Flink Sink 线程与 REST 查询线程之间的并发访问 */
    private final Object previewMonitor = new Object();

    // ==================== 指标计数（AtomicLong，支持多线程安全递增） ====================

    /** 从 Kafka 累计消费到的消息条数 */
    private final AtomicLong receivedCount = new AtomicLong();
    /** 成功写入 HBase 的消息条数 */
    private final AtomicLong writtenCount = new AtomicLong();
    /** 写入 HBase 失败的消息条数 */
    private final AtomicLong failedCount = new AtomicLong();
    /**
     * 最近成功写入 HBase 的数据行预览队列（双端队列）。
     * <p>
     * 新行从头部插入，超出 {@code previewSize} 时从尾部移除，
     * 实现固定大小的“最近 N 条”预览窗口。
     * </p>
     */
    private final Deque<KafkaToHBasePreviewRow> recentRows = new ArrayDeque<>();

    // ==================== 运行时状态（volatile，保证多线程可见性） ====================

    /** 当前正在运行的 Flink 作业 Future，为 null 表示未启动 */
    private volatile Future<?> runningFuture;
    /** Flink 作业客户端，用于查询状态和取消作业 */
    private volatile JobClient jobClient;
    /** 本次运行的唯一标识 ID，用于指标注册和日志溯源 */
    private volatile String runId;
    /** 管道当前状态：STOPPED / STARTING / RUNNING / FINISHED / FAILED */
    private volatile String state = "STOPPED";
    /** 管道状态的附加描述信息 */
    private volatile String message = "Pipeline is not running";
    /** Flink 作业 ID，由 JobManager 分配 */
    private volatile String jobId;
    /** 管道启动时间 */
    private volatile LocalDateTime startedAt;
    /** 管道停止时间；运行中时为 null */
    private volatile LocalDateTime stoppedAt;
    /** 最近一次写入失败的错误信息 */
    private volatile String lastError;

    /**
     * 构造服务实例，由 Spring 容器自动注入配置依赖。
     *
     * @param properties         管道配置参数
     * @param hbaseConfiguration Spring 容器中的 HBase 配置对象
     */
    public KafkaToHBasePipelineService(KafkaToHBasePipelineProperties properties,
                                       org.apache.hadoop.conf.Configuration hbaseConfiguration) {
        this.properties = properties;
        this.hbaseConfiguration = hbaseConfiguration;
    }

    /**
     * 启动 Kafka → HBase 数据管道。
     * <p>
     * 执行流程：
     * <ol>
     *   <li>校验必填配置项（Kafka 地址、Topic、GroupId、HBase 表名、列族）</li>
     *   <li>重置所有计数器和预览数据，确保每次启动是干净的</li>
     *   <li>生成新的 runId，并将自身注册到 {@link KafkaToHBasePipelineMetricsRegistry}</li>
     *   <li>创建 {@link KafkaToHBaseFlinkJob} 并提交到单线程池异步执行</li>
     * </ol>
     * 方法调用后立即返回当前状态快照，Flink 作业在后台启动。
     * </p>
     *
     * @return 启动后的管道状态快照
     * @throws BusinessException 若配置校验不通过或管道已在运行中时抛出
     */
    public KafkaToHBasePipelineStatus start() {
        // 校验必填配置项
        validateConfig();
        synchronized (lifecycleMonitor) {
            // 防止重复启动
            if (isRunning()) {
                throw new BusinessException("Kafka -> HBase Flink pipeline is already running");
            }
            // 重置指标计数器和预览数据
            receivedCount.set(0);
            writtenCount.set(0);
            failedCount.set(0);
            synchronized (previewMonitor) {
                recentRows.clear();
            }
            lastError = null;
            stoppedAt = null;
            startedAt = LocalDateTime.now();

            // 生成本次运行的唯一 ID，用于指标注册和日志溯源
            runId = UUID.randomUUID().toString();
            // 将自身注册为指标实例，Flink Sink 可通过 runId 获取并上报数据
            KafkaToHBasePipelineMetricsRegistry.register(runId, this);

            state = "STARTING";
            message = "Starting embedded Flink job";

            // 创建 Flink 作业并提交到后台线程异步执行
            KafkaToHBaseFlinkJob job = new KafkaToHBaseFlinkJob(properties, copyHBaseConfig(), runId);
            runningFuture = executorService.submit(() -> runJob(job));
            return status();
        }
    }

    /**
     * 停止 Kafka → HBase 数据管道。
     * <p>
     * 执行流程：
     * <ol>
     *   <li>通过 {@link JobClient#cancel()} 请求取消 Flink 作业</li>
     *   <li>更新状态为 STOPPED，记录停止时间</li>
     *   <li>中断后台执行线程</li>
     *   <li>从 {@link KafkaToHBasePipelineMetricsRegistry} 注销指标实例，释放资源</li>
     * </ol>
     * 若 Flink 作业已在停止过程中，取消失败不会抛出异常，仅记录警告日志。
     * </p>
     *
     * @return 停止后的管道状态快照
     */
    public KafkaToHBasePipelineStatus stop() {
        synchronized (lifecycleMonitor) {
            // 请求取消 Flink 作业；若作业已自行结束则取消操作无效果
            if (jobClient != null) {
                try {
                    jobClient.cancel().get();  // 阻塞等待取消完成
                } catch (Exception e) {
                    lastError = e.getMessage();
                    log.warn("Cancel Flink job failed: {}", e.getMessage());
                }
            }
            // 更新管道状态
            state = "STOPPED";
            message = "Stop requested";
            stoppedAt = LocalDateTime.now();

            // 中断后台执行线程（若仍在运行）
            if (runningFuture != null && !runningFuture.isDone()) {
                runningFuture.cancel(true);
            }
            runningFuture = null;
            jobClient = null;

            // 从注册中心注销指标实例，避免内存泄漏
            if (runId != null) {
                KafkaToHBasePipelineMetricsRegistry.unregister(runId);
            }
            return status();
        }
    }

    /**
     * 获取管道当前状态快照。
     * <p>
     * 聚合所有运行时状态字段为 {@link KafkaToHBasePipelineStatus} DTO，
     * 通过 REST 接口返回给前端，用于实时监控面板展示。
     * 预览列表在 {@code previewMonitor} 保护下进行快照复制，保证线程安全。
     * </p>
     *
     * @return 包含运行状态、计数指标、错误信息和预览数据的完整状态快照
     */
    public KafkaToHBasePipelineStatus status() {
        KafkaToHBasePipelineStatus status = new KafkaToHBasePipelineStatus();
        status.setRunning(isRunning());
        status.setState(state);
        status.setMessage(message);
        status.setJobId(jobId);
        status.setStartedAt(startedAt);
        status.setStoppedAt(stoppedAt);
        status.setReceivedCount(receivedCount.get());   // Kafka 消费计数
        status.setWrittenCount(writtenCount.get());      // HBase 写入成功计数
        status.setFailedCount(failedCount.get());        // HBase 写入失败计数
        status.setLastError(lastError);
        status.setConfig(properties);                    // 当前配置参数，便于运维确认
        // 对预览列表做快照复制，避免外部修改影响内部状态
        synchronized (previewMonitor) {
            status.setRecentRows(new ArrayList<>(recentRows));
        }
        return status;
    }

    /**
     * {@link KafkaToHBasePipelineMetrics} 接口实现：消息消费回调。
     * <p>由 Flink Sink 算子每消费一条 Kafka 消息时调用，接收计数器原子递增 +1。</p>
     */
    @Override
    public void onReceived() {
        receivedCount.incrementAndGet();
    }

    /**
     * {@link KafkaToHBasePipelineMetrics} 接口实现：写入成功回调。
     * <p>
     * 由 Flink Sink 算子每成功写入一条消息到 HBase 时调用。
     * 写入计数器原子递增 +1，并将预览行插入队列头部，
     * 超出 {@code previewSize} 限制时从尾部淘汰最旧的行。
     * </p>
     *
     * @param row 成功写入的预览行数据
     */
    @Override
    public void onWritten(KafkaToHBasePreviewRow row) {
        writtenCount.incrementAndGet();
        synchronized (previewMonitor) {
            recentRows.addFirst(row);  // 新行从头部插入
            // 超出预览窗口大小时，从尾部移除最旧的行
            while (recentRows.size() > Math.max(1, properties.getPreviewSize())) {
                recentRows.removeLast();
            }
        }
    }

    /**
     * {@link KafkaToHBasePipelineMetrics} 接口实现：写入失败回调。
     * <p>
     * 由 Flink Sink 算子写入 HBase 失败时调用。
     * 失败计数器原子递增 +1，并记录异常信息供前端展示。
     * </p>
     *
     * @param e 写入过程中抛出的异常
     */
    @Override
    public void onFailed(Exception e) {
        failedCount.incrementAndGet();
        lastError = e.getMessage();
    }

    /**
     * Spring 容器销毁时的回调方法，优雅停止管道并释放线程池。
     * <p>
     * 由 {@code @PreDestroy} 注解触发，确保 Spring 应用关闭时：
     * <ol>
     *   <li>调用 {@link #stop()} 取消 Flink 作业并注销指标实例</li>
     *   <li>调用 {@code shutdownNow()} 强制关闭执行线程池，防止线程泄漏</li>
     * </ol>
     * </p>
     */
    @PreDestroy
    public void destroy() {
        stop();
        executorService.shutdownNow();
    }

    /**
     * 在后台线程中执行 Flink 作业。
     * <p>
     * 该方法由 {@link #start()} 提交到单线程池异步运行，
     * 流程为：启动作业 → 阻塞等待作业结束。
     * 根据作业结束方式更新不同状态：
     * <ul>
     *   <li>正常结束 → {@code FINISHED}</li>
     *   <li>线程被中断（stop 触发） → {@code STOPPED}</li>
     *   <li>其他异常 → {@code FAILED}，并记录错误信息</li>
     * </ul>
     * 无论哪种结束方式，均在 finally 块中清理资源。
     * </p>
     *
     * @param job 待执行的 Flink 作业实例
     */
    private void runJob(KafkaToHBaseFlinkJob job) {
        try {
            state = "RUNNING";
            message = "Embedded Flink job is running";
            // 启动 Flink 作业（异步），获取 JobClient 用于后续监控和取消
            jobClient = job.start();
            jobId = jobClient.getJobID() == null ? null : jobClient.getJobID().toString();
            // 阻塞等待 Flink 作业执行完成（可能运行很长时间）
            JobExecutionResult result = jobClient.getJobExecutionResult().get();
            jobId = result.getJobID() == null ? null : result.getJobID().toString();
            state = "FINISHED";
            message = "Flink job finished";
        } catch (Throwable e) {
            // 线程被中断（由 stop() 触发），属于正常停止流程
            if (Thread.currentThread().isInterrupted()) {
                state = "STOPPED";
                message = "Flink job interrupted";
                return;
            }
            // 其他异常：作业启动失败或运行中出错
            lastError = e.getMessage();
            state = "FAILED";
            message = "Flink job failed: " + e.getMessage();
            log.error("Kafka -> HBase Flink pipeline failed", e);
        } finally {
            // 无论哪种结束方式，均执行清理操作
            stoppedAt = LocalDateTime.now();
            if (runId != null) {
                KafkaToHBasePipelineMetricsRegistry.unregister(runId);
            }
            synchronized (lifecycleMonitor) {
                runningFuture = null;
                jobClient = null;
            }
        }
    }

    /**
     * 判断管道是否正在运行。
     * <p>
     * 通过检查 {@code runningFuture} 是否为 null 且未完成来确定运行状态。
     * 覆盖的状态包括 STARTING、RUNNING。
     * </p>
     *
     * @return 若 Flink 作业正在执行中返回 {@code true}，否则返回 {@code false}
     */
    private boolean isRunning() {
        return runningFuture != null && !runningFuture.isDone();
    }

    /**
     * 校验管道启动所需的必填配置项。
     * <p>
     * 检查 Kafka Broker 地址、Topic、GroupId、HBase 表名和列族是否为空，
     * 任一为空则抛出 {@link BusinessException}，阻止管道启动。
     * </p>
     *
     * @throws BusinessException 若任一必填配置项为空时抛出
     */
    private void validateConfig() {
        if (StringUtils.isBlank(properties.getBootstrapServers())) {
            throw new BusinessException("Kafka bootstrap servers cannot be blank");
        }
        if (StringUtils.isBlank(properties.getTopic())) {
            throw new BusinessException("Kafka topic cannot be blank");
        }
        if (StringUtils.isBlank(properties.getGroupId())) {
            throw new BusinessException("Kafka group id cannot be blank");
        }
        if (StringUtils.isBlank(properties.getHbaseTable())) {
            throw new BusinessException("HBase target table cannot be blank");
        }
        if (StringUtils.isBlank(properties.getColumnFamily())) {
            throw new BusinessException("HBase column family cannot be blank");
        }
    }

    /**
     * 将 Spring 容器中的 HBase 配置复制为普通 Map。
     * <p>
     * Flink 作业运行在独立线程中，无法直接使用 Spring 的 HBase Configuration Bean，
     * 因此将必要的配置项提取为 {@code Map<String, String>} 传递给 Flink Sink。
     * 值为 null 的配置项会被自动过滤，避免传递无效参数。
     * </p>
     *
     * @return HBase 配置键值对 Map
     */
    private Map<String, String> copyHBaseConfig() {
        Map<String, String> values = new HashMap<>();
        // ZooKeeper 连接配置
        values.put("hbase.zookeeper.quorum", hbaseConfiguration.get("hbase.zookeeper.quorum"));
        values.put("hbase.zookeeper.property.clientPort", hbaseConfiguration.get("hbase.zookeeper.property.clientPort"));
        values.put("zookeeper.znode.parent", hbaseConfiguration.get("zookeeper.znode.parent"));
        // HBase 客户端行为配置
        values.put("hbase.client.retries.number", hbaseConfiguration.get("hbase.client.retries.number"));
        values.put("hbase.client.operation.timeout", hbaseConfiguration.get("hbase.client.operation.timeout"));
        values.put("hbase.rpc.timeout", hbaseConfiguration.get("hbase.rpc.timeout"));
        values.put("hbase.client.scanner.caching", hbaseConfiguration.get("hbase.client.scanner.caching"));
        // 过滤掉值为 null 的配置项，避免传递无效参数
        values.entrySet().removeIf(entry -> entry.getValue() == null);
        return values;
    }
}
