package com.example.hbase.experiments;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;

/**
 * 实验公共支撑工具。
 * <p>
 * 统一负责：实验表的生命周期管理（建表前清理、实验后可选清理）、
 * 耗时计量、分段日志输出，避免每个实验类重复样板代码。
 * <p>
 * 设计要点：
 * <ul>
 *   <li>所有实验表统一加 {@code exp_} 前缀，便于与业务表区分和批量清理。</li>
 *   <li>提供 {@link #run(String, ThrowingSupplier)} 包装实验执行，自动打印开始/结束/耗时。</li>
 *   <li>实验体允许抛出受检异常（HBase 操作普遍抛 IOException），由 run 统一包装为运行时异常。</li>
 *   <li>提供 {@link #safeDisableAndDelete(String)} 兜底清理，避免残留表影响下次实验。</li>
 * </ul>
 */
@Component
public class ExperimentSupport {

    private static final Logger log = LoggerFactory.getLogger(ExperimentSupport.class);

    /** 实验表统一前缀，便于识别和清理。 */
    public static final String TABLE_PREFIX = "exp_";

    /** 可抛受检异常的 Supplier，用于实验体（HBase 操作会抛 IOException）。 */
    @FunctionalInterface
    public interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    /** 可抛受检异常的 Runnable。 */
    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    @Autowired
    private Connection connection;

    /** 获取共享 HBase 连接（由 {@link com.example.hbase.config.HBaseConfig} 注入）。 */
    public Connection connection() {
        return connection;
    }

    /**
     * 执行一段实验逻辑，自动打印实验名、耗时。
     *
     * @param experimentName 实验名称，用于日志标识
     * @param action         实验体
     */
    public void run(String experimentName, ThrowingRunnable action) {
        run(experimentName, () -> {
            action.run();
            return null;
        });
    }

    /**
     * 执行一段实验逻辑并返回结果摘要。实验体允许抛出受检异常，
     * 内部捕获后包装为 {@link RuntimeException} 重新抛出，保持调用方代码简洁。
     *
     * @param experimentName 实验名称
     * @param action         实验体
     * @param <T>            结果类型
     * @return 实验结果
     */
    public <T> T run(String experimentName, ThrowingSupplier<T> action) {
        log.info("==================== [实验开始] {} ====================", experimentName);
        long start = System.currentTimeMillis();
        try {
            T result = action.get();
            long cost = System.currentTimeMillis() - start;
            log.info("==================== [实验结束] {} 耗时={}ms ====================", experimentName, cost);
            return result;
        } catch (Exception e) {
            long cost = System.currentTimeMillis() - start;
            log.error("==================== [实验异常] {} 耗时={}ms ====================", experimentName, cost, e);
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException("实验执行失败: " + experimentName, e);
        }
    }

    /**
     * 安全禁用并删除表。先 disable 再 delete，表不存在时直接返回。
     *
     * @param tableName 表名（不含前缀逻辑，由调用方决定）
     */
    public void safeDisableAndDelete(String tableName) {
        try (Admin admin = connection.getAdmin()) {
            TableName tn = TableName.valueOf(tableName);
            if (!admin.tableExists(tn)) {
                return;
            }
            if (admin.isTableEnabled(tn)) {
                admin.disableTable(tn);
            }
            admin.deleteTable(tn);
            log.info("已清理实验表: {}", tableName);
        } catch (IOException e) {
            log.warn("清理表 {} 失败: {}", tableName, e.getMessage());
        }
    }

    /**
     * 拼接实验表名，统一加前缀。
     */
    public String expTable(String suffix) {
        return TABLE_PREFIX + suffix;
    }

    /**
     * 打印分隔线 + 摘要信息，用于实验结果展示。
     */
    public void printSummary(String title, String... lines) {
        log.info("---------- {} ----------", title);
        if (lines != null) {
            Arrays.stream(lines).forEach(log::info);
        }
        log.info("----------------------------------------");
    }

    /**
     * 获取 Table 资源（调用方需 try-with-resources 关闭）。
     */
    public Table getTable(String tableName) throws IOException {
        return connection.getTable(TableName.valueOf(tableName));
    }

    /** 字符串转字节，简化实验代码。 */
    public static byte[] b(String s) {
        return Bytes.toBytes(s);
    }

    /** 字节转字符串，简化实验代码。 */
    public static String s(byte[] b) {
        return b == null ? null : Bytes.toString(b);
    }
}
