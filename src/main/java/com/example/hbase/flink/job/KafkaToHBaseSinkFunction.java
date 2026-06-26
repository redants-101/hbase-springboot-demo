package com.example.hbase.flink.job;

import com.example.hbase.flink.config.KafkaToHBasePipelineProperties;
import com.example.hbase.flink.dto.KafkaLogEvent;
import com.example.hbase.flink.dto.KafkaToHBasePreviewRow;
import com.example.hbase.flink.service.KafkaToHBasePipelineMetrics;
import com.example.hbase.flink.service.KafkaToHBasePipelineMetricsRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Flink Sink 算子：将 Kafka 日志事件实时写入 HBase。
 * <p>
 * 继承 {@link RichSinkFunction}，在 {@link #open} 阶段初始化 HBase 连接与表引用，
 * 在 {@link #invoke} 阶段完成单条消息的写入逻辑，在 {@link #close} 阶段释放资源。
 * </p>
 * <p>
 * 写入策略：
 * <ul>
 *   <li>RowKey 由 {@code topic-partition-offset} 组合生成，保证同一分区内有序且不重复</li>
 *   <li>消息原始内容存入 {@code raw} 列，Kafka 元数据存入 {@code kafka_*} 系列列</li>
 *   <li>若消息体为合法 JSON，则自动解析为扁平 KV 并逐列写入，便于后续 HBase 查询</li>
 *   <li>写入成功/失败的计数均通过 {@link KafkaToHBasePipelineMetrics} 上报，供前端实时监控</li>
 * </ul>
 * </p>
 */
public class KafkaToHBaseSinkFunction extends RichSinkFunction<KafkaLogEvent> {

    // ==================== HBase 列限定符常量 ====================

    /** 列限定符：消息来源的 Kafka Topic 名称 */
    private static final String META_TOPIC = "kafka_topic";
    /** 列限定符：消息所在的 Kafka 分区编号 */
    private static final String META_PARTITION = "kafka_partition";
    /** 列限定符：消息在分区内的偏移量 */
    private static final String META_OFFSET = "kafka_offset";
    /** 列限定符：Kafka 消息时间戳（毫秒） */
    private static final String META_TIMESTAMP = "kafka_timestamp";
    /** 列限定符：消息原始报文内容 */
    private static final String RAW = "raw";

    // ==================== 构造参数（序列化传输到 TaskManager） ====================

    /** 管道配置参数，包含 HBase 表名、列族、是否自动建表等 */
    private final KafkaToHBasePipelineProperties pipelineProperties;
    /** HBase 连接配置键值对（zk 地址、端口、超时等），在 open() 中注入到 HBaseConfiguration */
    private final Map<String, String> hbaseConfigValues;
    /** 本次运行的唯一标识，用于从 MetricsRegistry 获取对应的指标实例 */
    private final String runId;
    /** JSON 解析器，用于将 JSON 消息体解析为扁平 KV 列 */
    private final ObjectMapper objectMapper = new ObjectMapper();

    // ==================== 运行时资源（transient，不参与 Flink 序列化） ====================

    /** HBase 连接对象，在 open() 中创建，close() 中关闭 */
    private transient Connection connection;
    /** HBase 表操作句柄，在 open() 中获取，close() 中关闭 */
    private transient Table table;
    /** 管道运行时指标收集器，用于统计接收/写入/失败计数并维护预览数据 */
    private transient KafkaToHBasePipelineMetrics metrics;

    /**
     * 构造 Sink 算子实例。
     *
     * @param pipelineProperties 管道配置参数
     * @param hbaseConfigValues  HBase 连接配置键值对
     * @param runId              本次运行的唯一标识 ID
     */
    public KafkaToHBaseSinkFunction(KafkaToHBasePipelineProperties pipelineProperties,
                                    Map<String, String> hbaseConfigValues,
                                    String runId) {
        this.pipelineProperties = pipelineProperties;
        this.hbaseConfigValues = hbaseConfigValues;
        this.runId = runId;
    }

    /**
     * Sink 算子启动时调用，初始化 HBase 连接、确保目标表存在、获取指标实例。
     * <p>
     * 该方法在每个 TaskManager 的 SubTask 中独立执行一次。
     * 注意：{@code transient} 字段不参与 Flink 序列化，必须在 open() 中重新初始化。
     * </p>
     *
     * @param parameters Flink 算子配置参数（本实现未使用）
     * @throws Exception 若 HBase 连接创建失败或表不存在且未开启自动建表时抛出
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 从指标注册中心获取当前 runId 对应的指标实例，用于上报写入统计
        metrics = KafkaToHBasePipelineMetricsRegistry.get(runId);

        // 创建 HBase 配置并注入外部传入的 zk 地址、端口、超时等参数
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        for (Map.Entry<String, String> entry : hbaseConfigValues.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }

        // 建立与 HBase 集群的连接
        connection = ConnectionFactory.createConnection(conf);

        // 若开启自动建表且表不存在，则自动创建目标表
        ensureTable();

        // 获取目标表的引用，后续 invoke() 中复用该句柄执行写入
        table = connection.getTable(TableName.valueOf(pipelineProperties.getHbaseTable()));
    }

    /**
     * 每消费一条 Kafka 消息时调用，将消息写入 HBase。
     * <p>
     * 写入逻辑：
     * <ol>
     *   <li>生成 RowKey（topic-partition-offset），保证唯一且有序</li>
     *   <li>写入消息原始内容到 {@code raw} 列，以及 Kafka 元数据到 {@code kafka_*} 列</li>
     *   <li>尝试将消息体解析为 JSON，若成功则将扁平 KV 逐列写入，便于 HBase 按列查询</li>
     *   <li>写入成功后上报指标并生成预览行；写入失败时记录错误信息并抛出异常触发 Flink 重启策略</li>
     * </ol>
     * </p>
     *
     * @param event   从 Kafka 消费到的日志事件
     * @param context Flink Sink 上下文（包含处理时间、当前水位线等，本实现未使用）
     */
    @Override
    public void invoke(KafkaLogEvent event, Context context) {
        // 统计接收计数 +1
        if (metrics != null) {
            metrics.onReceived();
        }
        try {
            // 生成 RowKey：topic-partition-零填充offset，确保同一分区内有序
            String rowKey = buildRowKey(event);
            byte[] cf = Bytes.toBytes(pipelineProperties.getColumnFamily());

            // 构建 Put 请求：写入原始消息体和 Kafka 元数据列
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(cf, Bytes.toBytes(RAW), Bytes.toBytes(event.getValue()));            // 原始报文
            put.addColumn(cf, Bytes.toBytes(META_TOPIC), Bytes.toBytes(event.getTopic()));      // Topic
            put.addColumn(cf, Bytes.toBytes(META_PARTITION), Bytes.toBytes(String.valueOf(event.getPartition()))); // 分区号
            put.addColumn(cf, Bytes.toBytes(META_OFFSET), Bytes.toBytes(String.valueOf(event.getOffset())));       // 偏移量
            put.addColumn(cf, Bytes.toBytes(META_TIMESTAMP), Bytes.toBytes(String.valueOf(event.getKafkaTimestamp()))); // 时间戳

            // 尝试将消息体解析为 JSON，成功则将 KV 对逐列写入，便于按字段查询
            Map<String, String> parsedColumns = parseFlatJson(event.getValue());
            for (Map.Entry<String, String> entry : parsedColumns.entrySet()) {
                // 对列限定符做安全清洗，移除非法字符
                put.addColumn(cf, Bytes.toBytes(sanitizeQualifier(entry.getKey())), Bytes.toBytes(entry.getValue()));
            }

            // 执行 HBase 写入
            table.put(put);

            // 写入成功：上报指标并生成预览行供前端展示
            if (metrics != null) {
                metrics.onWritten(toPreviewRow(event, rowKey, parsedColumns));
            }
        } catch (Exception e) {
            // 写入失败：记录错误信息到指标（前端可展示），并抛出异常触发 Flink 失败重启策略
            if (metrics != null) {
                metrics.onFailed(e);
            }
            throw new RuntimeException("Write Kafka log event to HBase failed", e);
        }
    }

    /**
     * Sink 算子关闭时调用，释放 HBase 表和连接资源。
     * <p>
     * 在 Flink 作业取消、TaskManager 重启或正常结束时被调用。
     * 采用“先关 Table 再关 Connection”的顺序，避免资源泄漏。
     * </p>
     *
     * @throws Exception 若关闭过程中发生 IO 异常时抛出
     */
    @Override
    public void close() throws Exception {
        if (table != null) {
            table.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * 确保 HBase 目标表存在。
     * <p>
     * 仅当 {@code autoCreateTable=true} 时生效。若表不存在则自动创建，
     * 列族使用配置中指定的名称（默认 "info"）。
     * 若表已存在或未开启自动建表，则直接返回。
     * </p>
     *
     * @throws Exception 若 Admin 操作或建表失败时抛出
     */
    private void ensureTable() throws Exception {
        if (!pipelineProperties.isAutoCreateTable()) {
            return;
        }
        TableName tableName = TableName.valueOf(pipelineProperties.getHbaseTable());
        try (Admin admin = connection.getAdmin()) {
            if (admin.tableExists(tableName)) {
                return;  // 表已存在，无需创建
            }
            // 构建表描述：指定表名和列族，使用默认配置创建
            TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(pipelineProperties.getColumnFamily()));
            admin.createTable(tableBuilder.build());
        }
    }

    /**
     * 构建 HBase RowKey。
     * <p>
     * 格式为 {@code topic-partition-零填充offset}，例如：
     * {@code public-test-0-00000000000000001234}。
     * </p>
     * <ul>
     *   <li>Topic 前缀：区分不同数据源</li>
     *   <li>Partition 编号：保证同一分区的数据在 RowKey 层面连续</li>
     *   <li>零填充 offset（20位）：保证字符串字典序与数值序一致，有利于 HBase 范围扫描</li>
     * </ul>
     *
     * @param event Kafka 日志事件
     * @return 生成的 RowKey 字符串
     */
    private String buildRowKey(KafkaLogEvent event) {
        return event.getTopic() + "-" + event.getPartition() + "-" + String.format("%020d", event.getOffset());
    }

    /**
     * 将消息体尝试解析为扁平 JSON 键值对。
     * <p>
     * 若消息体是合法 JSON 对象，则解析为 {@code Map<String, String>}，
     * 嵌套对象或数组会被序列化为 JSON 字符串。若消息体非 JSON 或解析失败，
     * 则返回空 Map，不会抛出异常（容错设计，不影响原始数据的写入）。
     * </p>
     *
     * @param value Kafka 消息体原始字符串
     * @return 扁平化的列名→列值映射；非 JSON 或解析失败时返回空 Map
     */
    private Map<String, String> parseFlatJson(String value) {
        Map<String, String> columns = new LinkedHashMap<>();
        // 空值或纯空白字符串直接返回
        if (!StringUtils.hasText(value)) {
            return columns;
        }
        String trimmed = value.trim();
        // 非 JSON 对象格式（不以 { 开头或 } 结尾）直接返回
        if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
            return columns;
        }
        try {
            // 解析 JSON 为 Map<String, Object>
            Map<String, Object> parsed = objectMapper.readValue(trimmed, new TypeReference<Map<String, Object>>() {
            });
            for (Map.Entry<String, Object> entry : parsed.entrySet()) {
                Object item = entry.getValue();
                if (item == null || item instanceof Map || item instanceof Iterable) {
                    // null、嵌套对象、数组 → 序列化为 JSON 字符串存储
                    columns.put(entry.getKey(), objectMapper.writeValueAsString(item));
                } else {
                    // 基本类型（String/Number/Boolean）→ 直接转字符串
                    columns.put(entry.getKey(), String.valueOf(item));
                }
            }
        } catch (Exception ignored) {
            // JSON 解析失败时静默忽略，不影响原始数据的写入
            return new LinkedHashMap<>();
        }
        return columns;
    }

    /**
     * 清洗 HBase 列限定符，将非法字符替换为下划线。
     * <p>
     * HBase 列限定符仅允许字母、数字、下划线、点和短横线，
     * 其他字符（如空格、中文、特殊符号）均会被替换为 {@code _}，
     * 防止写入时因列名不合法导致异常。
     * </p>
     *
     * @param qualifier 原始列限定符名称
     * @return 清洗后的合法列限定符
     */
    private String sanitizeQualifier(String qualifier) {
        return qualifier.replaceAll("[^a-zA-Z0-9_.-]", "_");
    }

    /**
     * 将写入成功的事件转换为前端预览行 DTO。
     *
     * @param event         原始 Kafka 日志事件
     * @param rowKey        生成的 HBase RowKey
     * @param parsedColumns 解析后的 JSON 列映射
     * @return 前端预览用的数据行对象
     */
    private KafkaToHBasePreviewRow toPreviewRow(KafkaLogEvent event, String rowKey, Map<String, String> parsedColumns) {
        KafkaToHBasePreviewRow row = new KafkaToHBasePreviewRow();
        row.setRowKey(rowKey);
        row.setRaw(event.getValue());
        row.setTopic(event.getTopic());
        row.setPartition(event.getPartition());
        row.setOffset(event.getOffset());
        row.setWrittenAt(LocalDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.systemDefault()));
        row.setParsedColumns(parsedColumns);
        return row;
    }
}
