package com.example.hbase.flink.controller;

import com.example.hbase.dto.ApiResponse;
import com.example.hbase.flink.dto.KafkaToHBasePipelineStatus;
import com.example.hbase.flink.service.KafkaToHBasePipelineService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Kafka → HBase 实时数据管道的 REST 控制器。
 * <p>
 * 提供管道生命周期管理的 HTTP 接口，供前端监控页面或运维工具调用：
 * <ul>
 *   <li>{@code POST /api/pipeline/kafka-hbase/start} —— 启动管道，开始消费 Kafka 并写入 HBase</li>
 *   <li>{@code POST /api/pipeline/kafka-hbase/stop}  —— 停止管道，取消 Flink 作业</li>
 *   <li>{@code GET  /api/pipeline/kafka-hbase/status}—— 查询管道当前状态、指标和预览数据</li>
 * </ul>
 * 所有接口均通过 {@link ApiResponse} 统一封装返回，
 * 业务异常由 {@link com.example.hbase.exception.GlobalExceptionHandler} 全局拦截处理。
 * </p>
 *
 * @see KafkaToHBasePipelineService
 * @see com.example.hbase.dto.ApiResponse
 */
@RestController
@RequestMapping("/api/pipeline/kafka-hbase")
public class KafkaToHBasePipelineController {

    /** 管道管理服务，负责 Flink 作业的启停和状态查询 */
    private final KafkaToHBasePipelineService pipelineService;

    /**
     * 构造控制器实例，由 Spring 自动注入管道管理服务。
     *
     * @param pipelineService 管道管理服务
     */
    public KafkaToHBasePipelineController(KafkaToHBasePipelineService pipelineService) {
        this.pipelineService = pipelineService;
    }

    /**
     * 启动 Kafka → HBase 数据管道。
     * <p>
     * 调用 {@link KafkaToHBasePipelineService#start()} 异步启动 Flink 作业，
     * 方法立即返回启动后的状态快照。若管道已在运行中，
     * Service 层会抛出 {@link com.example.hbase.exception.BusinessException}，
     * 由全局异常处理器返回错误响应。
     * </p>
     *
     * @return 包含管道启动状态（STARTING）的统一响应
     */
    @PostMapping("/start")
    public ApiResponse<KafkaToHBasePipelineStatus> start() {
        return ApiResponse.success(pipelineService.start());
    }

    /**
     * 停止 Kafka → HBase 数据管道。
     * <p>
     * 调用 {@link KafkaToHBasePipelineService#stop()} 取消 Flink 作业，
     * 返回停止后的状态快照（包含累计指标和最近预览数据）。
     * </p>
     *
     * @return 包含管道停止状态（STOPPED）的统一响应
     */
    @PostMapping("/stop")
    public ApiResponse<KafkaToHBasePipelineStatus> stop() {
        return ApiResponse.success(pipelineService.stop());
    }

    /**
     * 查询 Kafka → HBase 数据管道的当前状态。
     * <p>
     * 返回完整的状态快照，包括：运行状态、Flink 作业 ID、启停时间、
     * 消费/写入/失败计数、最近错误信息、当前配置参数和最近写入预览数据。
     * 前端监控页面通过定时轮询该接口实现实时状态展示。
     * </p>
     *
     * @return 包含管道完整状态快照的统一响应
     */
    @GetMapping("/status")
    public ApiResponse<KafkaToHBasePipelineStatus> status() {
        return ApiResponse.success(pipelineService.status());
    }
}
