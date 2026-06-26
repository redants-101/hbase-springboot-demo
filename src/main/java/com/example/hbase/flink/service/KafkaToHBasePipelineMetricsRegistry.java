package com.example.hbase.flink.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Kafka → HBase 管道指标实例的全局注册中心。
 * <p>
 * 采用单例 + 静态工具类设计，内部使用 {@link ConcurrentHashMap} 维护
 * {@code runId → KafkaToHBasePipelineMetrics} 的映射关系。
 * </p>
 * <p>
 * <b>设计背景：</b>
 * Flink Sink 算子运行在 TaskManager 的独立线程中，无法直接注入 Spring Bean；
 * 而前端监控接口又需要从 Spring Controller 层读取指标数据。
 * 本注册中心作为跨线程共享桥梁，让双方通过相同的 {@code runId} 访问同一指标实例：
 * <ul>
 *   <li>{@code KafkaToHBasePipelineService} 在启动管道时创建指标实例并调用 {@link #register} 注册</li>
 *   <li>{@code KafkaToHBaseSinkFunction} 在 {@code open()} 阶段调用 {@link #get} 获取实例上报指标</li>
 *   <li>{@code KafkaToHBasePipelineController} 调用 {@link #get} 查询当前状态并返回给前端</li>
 *   <li>管道停止后调用 {@link #unregister} 释放资源，避免内存泄漏</li>
 * </ul>
 * </p>
 *
 * @see KafkaToHBasePipelineMetrics
 */
public final class KafkaToHBasePipelineMetricsRegistry {

    /**
     * 指标实例注册表：runId → 指标实例。
     * <p>
     * 使用 {@link ConcurrentHashMap} 保证多线程环境下的读写安全，
     * Flink TaskManager 线程与 Spring 主线程可并发访问。
     * </p>
     */
    private static final Map<String, KafkaToHBasePipelineMetrics> REGISTRY = new ConcurrentHashMap<>();

    /**
     * 私有构造函数，防止外部实例化（工具类单例模式）。
     */
    private KafkaToHBasePipelineMetricsRegistry() {
    }

    /**
     * 将指标实例注册到全局注册中心。
     * <p>
     * 由 {@code KafkaToHBasePipelineService} 在管道启动时调用，
     * 将新创建的指标实例与本次运行的 {@code runId} 绑定。
     * 若相同 {@code runId} 已存在，则覆盖旧实例。
     * </p>
     *
     * @param runId   本次管道运行的唯一标识 ID
     * @param metrics 待注册的指标实例
     */
    public static void register(String runId, KafkaToHBasePipelineMetrics metrics) {
        REGISTRY.put(runId, metrics);
    }

    /**
     * 根据 runId 获取已注册的指标实例。
     * <p>
     * 主要调用方：
     * <ul>
     *   <li>{@code KafkaToHBaseSinkFunction#open()} —— Flink 算子启动时获取实例用于上报指标</li>
     *   <li>{@code KafkaToHBasePipelineController} —— REST 接口查询当前管道状态时获取实例</li>
     * </ul>
     * 若 {@code runId} 未注册，返回 {@code null}。
     * </p>
     *
     * @param runId 本次管道运行的唯一标识 ID
     * @return 对应的指标实例；若未注册则返回 {@code null}
     */
    public static KafkaToHBasePipelineMetrics get(String runId) {
        return REGISTRY.get(runId);
    }

    /**
     * 从注册中心移除指定 runId 对应的指标实例。
     * <p>
     * 由 {@code KafkaToHBasePipelineService} 在管道停止或异常结束时调用，
     * 释放指标实例占用的内存，避免长时间运行后注册表无限增长导致内存泄漏。
     * 若 {@code runId} 不存在，调用不产生任何效果。
     * </p>
     *
     * @param runId 本次管道运行的唯一标识 ID
     */
    public static void unregister(String runId) {
        REGISTRY.remove(runId);
    }
}
