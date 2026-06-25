package com.example.hbase.experiments.table;

import com.example.hbase.experiments.ExperimentSupport;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * 实验一：表设计属性（列族配置 / 版本控制 / TTL / KEEP_DELETED_CELLS）。
 * <p>
 * 覆盖 HBase 表设计中最核心的几个列族级属性：
 * <ul>
 *   <li>{@code MAX_VERSIONS}：单 Cell 保留的最大历史版本数</li>
 *   <li>{@code MIN_VERSIONS}：TTL 触发后仍保留的最小版本数（与 TTL 配合）</li>
 *   <li>{@code TTL}：数据存活时间（秒），过期版本在 compaction 时被物理删除</li>
 *   <li>{@code KEEP_DELETED_CELLS}：是否保留被删除的 Cell，用于时间旅行查询</li>
 * </ul>
 * <p>
 * 运行前提：可访问的 HBase 集群（Docker 单机即可），实验表会自动创建和清理。
 */
@Component
public class TableDesignExperiments {

    private static final Logger log = LoggerFactory.getLogger(TableDesignExperiments.class);

    @Autowired
    private ExperimentSupport support;

    /**
     * 实验 1.1：列族属性读取与对比。
     * <p>
     * <b>参数配置说明</b>：通过 {@link ColumnFamilyDescriptorBuilder} 显式设置
     * {@code MAX_VERSIONS=3}、{@code TTL=86400}（1天）、{@code BLOCKSIZE=65536}，
     * 与默认列族（{@code MAX_VERSIONS=1}）对比。
     * <p>
     * <b>预期结果</b>：建表后通过 {@link Admin#getDescriptor(TableName)} 读取到的
     * 列族属性应与设置值一致；默认列族 MAX_VERSIONS 应为 1。
     * <p>
     * <b>对比验证方法</b>：打印两个列族的属性，人工核对。
     */
    public void experimentColumnFamilyAttributes() {
        support.run("1.1 列族属性读取与对比", () -> {
            String table = support.expTable("cf_attrs");
            support.safeDisableAndDelete(table);
            try (Admin admin = support.connection().getAdmin()) {
                TableName tn = TableName.valueOf(table);
                ColumnFamilyDescriptor customCf = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf_custom"))
                        .setMaxVersions(3)
                        .setTimeToLive(86400)
                        .setBlocksize(65536)
                        .build();
                ColumnFamilyDescriptor defaultCf = ColumnFamilyDescriptorBuilder.of("cf_default");

                TableDescriptor td = TableDescriptorBuilder.newBuilder(tn)
                        .setColumnFamily(customCf)
                        .setColumnFamily(defaultCf)
                        .build();
                admin.createTable(td);

                TableDescriptor loaded = admin.getDescriptor(tn);
                ColumnFamilyDescriptor loadedCustom = loaded.getColumnFamily(Bytes.toBytes("cf_custom"));
                ColumnFamilyDescriptor loadedDefault = loaded.getColumnFamily(Bytes.toBytes("cf_default"));

                support.printSummary("列族属性对比",
                        "cf_custom  MAX_VERSIONS = " + loadedCustom.getMaxVersions() + " (期望 3)",
                        "cf_custom  TTL          = " + loadedCustom.getTimeToLive() + " (期望 86400)",
                        "cf_custom  BLOCKSIZE    = " + loadedCustom.getBlocksize() + " (期望 65536)",
                        "cf_default MAX_VERSIONS = " + loadedDefault.getMaxVersions() + " (期望 1)",
                        "cf_default TTL          = " + loadedDefault.getTimeToLive() + " (期望 FOREVER=2147483647)");
            }
            return null;
        });
    }

    /**
     * 实验 1.2：多版本控制（MAX_VERSIONS）。
     * <p>
     * <b>原理</b>：HBase 的每个 Cell 默认只保留最新版本（MAX_VERSIONS=1）。
     * 设置 MAX_VERSIONS=N 后，对同一 rowKey+cf+qualifier 多次 put，
     * 会保留最近 N 个版本，按 timestamp 倒序排列。
     * <p>
     * <b>参数配置</b>：列族 {@code cf_v} 设置 {@code MAX_VERSIONS=3}。
     * <p>
     * <b>实验步骤</b>：对同一 Cell 连续写入 4 个版本（v1<v2<v3<v4），用 {@link Get#readAllVersions()} 读取。
     * <p>
     * <b>预期结果</b>：只能读到 3 个版本（v2、v3、v4），v1 被淘汰。
     * <p>
     * <b>对比验证</b>：将 MAX_VERSIONS 改为 1，重做实验，应只读到 v4。
     */
    public void experimentMaxVersions() {
        support.run("1.2 多版本控制 MAX_VERSIONS", () -> {
            String table = support.expTable("versions");
            support.safeDisableAndDelete(table);
            try (Admin admin = support.connection().getAdmin()) {
                TableName tn = TableName.valueOf(table);
                ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf_v"))
                        .setMaxVersions(3)
                        .build();
                admin.createTable(TableDescriptorBuilder.newBuilder(tn).setColumnFamily(cf).build());
            }
            try (Table t = support.getTable(table)) {
                byte[] row = Bytes.toBytes("row1");
                byte[] cf = Bytes.toBytes("cf_v");
                byte[] q = Bytes.toBytes("name");
                long base = System.currentTimeMillis();
                for (int i = 1; i <= 4; i++) {
                    Put put = new Put(row);
                    put.addColumn(cf, q, base + i, Bytes.toBytes("v" + i));
                    t.put(put);
                }
                Get get = new Get(row).readAllVersions();
                Result result = t.get(get);
                int versionCount = result.rawCells().length;
                StringBuilder versions = new StringBuilder();
                Arrays.stream(result.rawCells()).forEach(c ->
                        versions.append(Bytes.toString(org.apache.hadoop.hbase.CellUtil.cloneValue(c))).append(" "));
                support.printSummary("多版本读取结果",
                        "实际读取版本数 = " + versionCount + " (期望 3，因 MAX_VERSIONS=3 且写入4个)",
                        "版本值（按ts降序）= " + versions.toString().trim() + " (期望 v4 v3 v2)",
                        "结论：超出 MAX_VERSIONS 的最旧版本 v1 被自动淘汰");
            }
            return null;
        });
    }

    /**
     * 实验 1.3：TTL 数据过期。
     * <p>
     * <b>原理</b>：TTL（Time To Live）以秒为单位，过期的 Cell 在 major compaction
     * 时被物理删除。读取时也会实时过滤过期版本。
     * <p>
     * <b>参数配置</b>：TTL=5 秒，便于快速观察。
     * <p>
     * <b>实验步骤</b>：写入数据 → 立即读取（应存在）→ 等待 6 秒 → 再次读取（应为空）。
     * <p>
     * <b>预期结果</b>：第一次读到值，第二次读到 null。
     */
    public void experimentTTL() {
        support.run("1.3 TTL 数据过期", () -> {
            String table = support.expTable("ttl");
            support.safeDisableAndDelete(table);
            try (Admin admin = support.connection().getAdmin()) {
                TableName tn = TableName.valueOf(table);
                ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf_ttl"))
                        .setTimeToLive(5)
                        .setMaxVersions(1)
                        .build();
                admin.createTable(TableDescriptorBuilder.newBuilder(tn).setColumnFamily(cf).build());
            }
            try (Table t = support.getTable(table)) {
                byte[] row = Bytes.toBytes("r1");
                byte[] cf = Bytes.toBytes("cf_ttl");
                byte[] q = Bytes.toBytes("data");
                t.put(new Put(row).addColumn(cf, q, Bytes.toBytes("hello-ttl")));

                String first = readCell(t, row, cf, q);
                log.info("写入后立即读取: {} (期望 hello-ttl)", first);

                log.info("等待 6 秒，让数据超过 TTL=5s ...");
                TimeUnit.SECONDS.sleep(6);

                String second = readCell(t, row, cf, q);
                support.printSummary("TTL 过期验证",
                        "6秒后读取: " + second + " (期望 null，已被 TTL 过滤)",
                        "结论：TTL 在读取时实时生效，无需等待 compaction");
            }
            return null;
        });
    }

    /**
     * 实验 1.4：MIN_VERSIONS 与 TTL 配合。
     * <p>
     * <b>原理</b>：当 TTL 到期，正常会删除所有过期版本。但若设置了 MIN_VERSIONS=N，
     * 即使全部过期，也会保留最近 N 个版本，用于"即使过期也要留底"的场景。
     * <p>
     * <b>参数配置</b>：MAX_VERSIONS=3, MIN_VERSIONS=1, TTL=5。
     * <p>
     * <b>预期结果</b>：TTL 过期后，仍能读到 1 个版本（MIN_VERSIONS 兜底）。
     * <p>
     * <b>注意</b>：MIN_VERSIONS 兜底依赖 major compaction 时机，本实验主要验证配置生效。
     */
    public void experimentMinVersions() {
        support.run("1.4 MIN_VERSIONS 与 TTL 配合", () -> {
            String table = support.expTable("min_versions");
            support.safeDisableAndDelete(table);
            try (Admin admin = support.connection().getAdmin()) {
                TableName tn = TableName.valueOf(table);
                ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf_mv"))
                        .setMaxVersions(3)
                        .setMinVersions(1)
                        .setTimeToLive(5)
                        .build();
                admin.createTable(TableDescriptorBuilder.newBuilder(tn).setColumnFamily(cf).build());
            }
            try (Table t = support.getTable(table)) {
                byte[] row = Bytes.toBytes("r1");
                byte[] cf = Bytes.toBytes("cf_mv");
                byte[] q = Bytes.toBytes("data");
                long base = System.currentTimeMillis();
                for (int i = 1; i <= 3; i++) {
                    t.put(new Put(row).addColumn(cf, q, base + i, Bytes.toBytes("v" + i)));
                }
                log.info("写入 3 个版本，等待 6 秒让 TTL 过期 ...");
                TimeUnit.SECONDS.sleep(6);

                Get get = new Get(row).readAllVersions();
                Result result = t.get(get);
                int count = result.rawCells().length;
                support.printSummary("MIN_VERSIONS 验证",
                        "过期后读取版本数 = " + count + " (期望 >=1，MIN_VERSIONS=1 兜底)",
                        "说明：MIN_VERSIONS 保证即使 TTL 过期也保留 N 个版本用于留底",
                        "注意：实际是否保留还取决于 major compaction 是否执行");
            }
            return null;
        });
    }

    /**
     * 实验 1.5：KEEP_DELETED_CELLS。
     * <p>
     * <b>原理</b>：默认情况下，HBase 删除一个 Cell 后，该 Cell 在 compaction 时被清除。
     * 设置 KEEP_DELETED_CELLS=true 后，被删除的 Cell 会被保留，可通过指定过去 timestamp
     * 的 Get/Scan 实现"时间旅行"查询。
     * <p>
     * <b>参数配置</b>：KEEP_DELETED_CELLS=true, MAX_VERSIONS=3。
     * <p>
     * <b>预期结果</b>：删除后用写入时刻的 ts 范围扫描，仍能查到 v1。
     */
    public void experimentKeepDeletedCells() {
        support.run("1.5 KEEP_DELETED_CELLS 时间旅行", () -> {
            String table = support.expTable("keep_deleted");
            support.safeDisableAndDelete(table);
            try (Admin admin = support.connection().getAdmin()) {
                TableName tn = TableName.valueOf(table);
                ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf_kd"))
                        .setMaxVersions(3)
                        .setKeepDeletedCells(org.apache.hadoop.hbase.KeepDeletedCells.TRUE)
                        .build();
                admin.createTable(TableDescriptorBuilder.newBuilder(tn).setColumnFamily(cf).build());
            }
            try (Table t = support.getTable(table)) {
                byte[] row = Bytes.toBytes("r1");
                byte[] cf = Bytes.toBytes("cf_kd");
                byte[] q = Bytes.toBytes("data");
                long writeTs = System.currentTimeMillis();
                t.put(new Put(row).addColumn(cf, q, writeTs, Bytes.toBytes("v1")));

                long deleteTs = writeTs + 1000;
                org.apache.hadoop.hbase.client.Delete del = new org.apache.hadoop.hbase.client.Delete(row);
                del.addColumns(cf, q, deleteTs);
                t.delete(del);

                String current = readCell(t, row, cf, q);
                log.info("删除后当前读取: {} (期望 null)", current);

                Scan scan = new Scan().withStartRow(row).withStopRow(row, true)
                        .readAllVersions()
                        .setTimeRange(0, writeTs + 1);
                int found = 0;
                try (var scanner = t.getScanner(scan)) {
                    for (Result r : scanner) {
                        found += r.rawCells().length;
                    }
                }
                support.printSummary("KEEP_DELETED_CELLS 验证",
                        "时间旅行扫描到的 Cell 数 = " + found + " (期望 >=1，删除前的 v1 仍可查)",
                        "结论：KEEP_DELETED_CELLS=true 保留了被删除 Cell，支持按历史时间点查询",
                        "应用场景：审计回溯、误删恢复、增量同步");
            }
            return null;
        });
    }

    private String readCell(Table t, byte[] row, byte[] cf, byte[] q) throws Exception {
        Result r = t.get(new Get(row).addColumn(cf, q));
        if (r.isEmpty()) {
            return null;
        }
        return Bytes.toString(r.getValue(cf, q));
    }
}
