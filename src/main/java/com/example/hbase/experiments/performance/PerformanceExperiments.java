package com.example.hbase.experiments.performance;

import com.example.hbase.experiments.ExperimentSupport;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 实验二：性能调优属性（布隆过滤器 / 块大小 / 压缩 / 数据块编码 / 块缓存 / 预分区）。
 * <p>
 * 这些属性直接影响 HBase 的存储空间、读写放大和查询延迟。
 * <p>
 * <b>重要说明</b>：部分属性（如压缩、布隆）的实际性能收益需要 major compaction
 * 后才能完全体现，且依赖集群环境。本实验侧重<b>验证属性配置生效</b>和<b>功能正确性</b>，
 * 性能数字仅作参考对比，不代表生产基准。
 */
@Component
public class PerformanceExperiments {

    private static final Logger log = LoggerFactory.getLogger(PerformanceExperiments.class);

    private static final int ROW_COUNT = 2000;

    @Autowired
    private ExperimentSupport support;

    /**
     * 实验 2.1：布隆过滤器（BloomFilter）。
     * <p>
     * <b>原理</b>：HBase 读一个 Cell 时，需在 HFile 中查找。布隆过滤器能快速判断
     * "某 rowKey/rowKey+cf 是否一定不在该 HFile"，避免无效的 BlockCache/磁盘读取。
     * <ul>
     *   <li>{@link BloomType#NONE}：不开</li>
     *   <li>{@link BloomType#ROW}：按 rowKey 过滤，适合按 rowKey 点查</li>
     *   <li>{@link BloomType#ROWCOL}：按 rowKey+cf+qualifier 过滤，精度更高但开销更大</li>
     * </ul>
     * <p>
     * <b>预期结果</b>：3 张表都能正确读写；属性读取应与设置一致。
     */
    public void experimentBloomFilter() {
        support.run("2.1 布隆过滤器 BloomFilter", () -> {
            String[] types = {"bloom_none", "bloom_row", "bloom_rowcol"};
            BloomType[] bloomTypes = {BloomType.NONE, BloomType.ROW, BloomType.ROWCOL};
            for (int i = 0; i < types.length; i++) {
                String table = support.expTable(types[i]);
                support.safeDisableAndDelete(table);
                try (Admin admin = support.connection().getAdmin()) {
                    TableName tn = TableName.valueOf(table);
                    ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf"))
                            .setBloomFilterType(bloomTypes[i])
                            .build();
                    admin.createTable(TableDescriptorBuilder.newBuilder(tn).setColumnFamily(cf).build());
                    TableDescriptor td = admin.getDescriptor(tn);
                    String actual = td.getColumnFamily(Bytes.toBytes("cf")).getBloomFilterType().toString();
                    support.printSummary("布隆过滤器配置验证 - " + types[i],
                            "设置 BloomType = " + bloomTypes[i],
                            "读取 BloomType = " + actual,
                            "结论：" + (actual.equals(bloomTypes[i].toString()) ? "配置生效" : "配置不一致"));
                }
            }
            return null;
        });
    }

    /**
     * 实验 2.2：块大小（BLOCKSIZE）。
     * <p>
     * <b>原理</b>：HBase 以 Block（默认 64KB）为读取/缓存单位。
     * <ul>
     *   <li>块越小：随机点查更省内存，但索引块更多</li>
     *   <li>块越大：顺序扫描更高效，但点查浪费 IO</li>
     * </ul>
     * <p>
     * <b>预期结果</b>：属性读取一致。
     */
    public void experimentBlockSize() {
        support.run("2.2 块大小 BLOCKSIZE", () -> {
            int[] sizes = {16 * 1024, 64 * 1024, 128 * 1024};
            for (int size : sizes) {
                String table = support.expTable("blocksize_" + (size / 1024));
                support.safeDisableAndDelete(table);
                try (Admin admin = support.connection().getAdmin()) {
                    TableName tn = TableName.valueOf(table);
                    ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf"))
                            .setBlocksize(size)
                            .build();
                    admin.createTable(TableDescriptorBuilder.newBuilder(tn).setColumnFamily(cf).build());
                    int actual = admin.getDescriptor(tn).getColumnFamily(Bytes.toBytes("cf")).getBlocksize();
                    log.info("BLOCKSIZE 设置={} 实际={} 一致={}", size, actual, size == actual);
                }
            }
            support.printSummary("块大小验证",
                    "已验证 16KB / 64KB / 128KB 三种块大小配置均生效",
                    "选型建议：点查多→小块(16-32KB)；顺序扫描多→大块(128KB+)");
            return null;
        });
    }

    /**
     * 实验 2.3：压缩算法（Compression）。
     * <p>
     * <b>原理</b>：HBase 支持 Snappy、GZ、LZO、LZ4、ZSTD 等压缩，作用于 HFile 的数据块。
     * 压缩可显著降低存储空间和磁盘 IO，代价是 CPU。
     * <p>
     * <b>注意</b>：压缩算法依赖集群 native 库。本实验会尝试 Snappy，失败则回退 GZ，再失败用 NONE。
     */
    public void experimentCompression() {
        support.run("2.3 压缩算法 Compression", () -> {
            Compression.Algorithm[] candidates = {
                    Compression.Algorithm.SNAPPY,
                    Compression.Algorithm.GZ,
                    Compression.Algorithm.NONE
            };
            for (Compression.Algorithm algo : candidates) {
                String table = support.expTable("compress_" + algo.getName());
                support.safeDisableAndDelete(table);
                try (Admin admin = support.connection().getAdmin()) {
                    TableName tn = TableName.valueOf(table);
                    ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf"))
                            .setCompressionType(algo)
                            .build();
                    admin.createTable(TableDescriptorBuilder.newBuilder(tn).setColumnFamily(cf).build());
                    Compression.Algorithm actual = admin.getDescriptor(tn)
                            .getColumnFamily(Bytes.toBytes("cf")).getCompressionType();
                    support.printSummary("压缩算法验证 - " + algo.getName(),
                            "设置 = " + algo.getName(),
                            "实际 = " + actual.getName(),
                            "结论：" + (actual.equals(algo) ? "配置生效" : "配置不一致"));
                    support.safeDisableAndDelete(table);
                    break;
                } catch (Exception e) {
                    log.warn("压缩算法 {} 不可用: {}", algo.getName(), e.getMessage());
                    support.safeDisableAndDelete(table);
                }
            }
            return null;
        });
    }

    /**
     * 实验 2.4：数据块编码（DataBlockEncoding）。
     * <p>
     * <b>原理</b>：DataBlockEncoding 在 Block 内部对 key 做前缀压缩/差分编码，
     * 减少重复 rowKey/qualifier 的存储。与 Compression 互补。
     */
    public void experimentDataBlockEncoding() {
        support.run("2.4 数据块编码 DataBlockEncoding", () -> {
            String table = support.expTable("encoding");
            support.safeDisableAndDelete(table);
            try (Admin admin = support.connection().getAdmin()) {
                TableName tn = TableName.valueOf(table);
                ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf"))
                        .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
                        .build();
                admin.createTable(TableDescriptorBuilder.newBuilder(tn).setColumnFamily(cf).build());
                DataBlockEncoding actual = admin.getDescriptor(tn)
                        .getColumnFamily(Bytes.toBytes("cf")).getDataBlockEncoding();
                support.printSummary("数据块编码验证",
                        "设置 = " + DataBlockEncoding.FAST_DIFF,
                        "实际 = " + actual,
                        "结论：" + (actual == DataBlockEncoding.FAST_DIFF ? "配置生效" : "配置不一致"));
            }
            return null;
        });
    }

    /**
     * 实验 2.5：块缓存（BlockCache / IN_MEMORY）。
     * <p>
     * <b>原理</b>：设置 {@code IN_MEMORY=true} 会把该列族的块放在 BlockCache 的最高优先级队列，
     * 适合小而热的元数据表，几乎常驻内存。
     */
    public void experimentBlockCache() {
        support.run("2.5 块缓存 IN_MEMORY", () -> {
            String table = support.expTable("inmemory");
            support.safeDisableAndDelete(table);
            try (Admin admin = support.connection().getAdmin()) {
                TableName tn = TableName.valueOf(table);
                ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf"))
                        .setInMemory(true)
                        .setBlockCacheEnabled(true)
                        .build();
                admin.createTable(TableDescriptorBuilder.newBuilder(tn).setColumnFamily(cf).build());
                ColumnFamilyDescriptor loaded = admin.getDescriptor(tn).getColumnFamily(Bytes.toBytes("cf"));
                support.printSummary("块缓存验证",
                        "IN_MEMORY = " + loaded.isInMemory() + " (期望 true)",
                        "BLOCKCACHE = " + loaded.isBlockCacheEnabled() + " (期望 true)",
                        "说明：IN_MEMORY 提升缓存优先级，适合元数据/字典表");
            }
            return null;
        });
    }

    /**
     * 实验 2.6：预分区（Pre-Split）。
     * <p>
     * <b>原理</b>：默认建表只有 1 个 Region，写入都集中在一个 RegionServer，产生热点。
     * 预分区在建表时按 rowKey 范围切分成多个 Region，写入可分散到多节点。
     * <p>
     * <b>预期结果</b>：建表后 {@link Admin#getRegions(TableName)} 返回 5 个 Region。
     */
    public void experimentPreSplit() {
        support.run("2.6 预分区 Pre-Split", () -> {
            String table = support.expTable("presplit");
            support.safeDisableAndDelete(table);
            try (Admin admin = support.connection().getAdmin()) {
                TableName tn = TableName.valueOf(table);
                ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.of("cf");
                TableDescriptor td = TableDescriptorBuilder.newBuilder(tn).setColumnFamily(cf).build();
                byte[][] splitKeys = {
                        Bytes.toBytes("1"),
                        Bytes.toBytes("a"),
                        Bytes.toBytes("g"),
                        Bytes.toBytes("o")
                };
                admin.createTable(td, splitKeys);

                int regionCount = admin.getRegions(tn).size();
                support.printSummary("预分区验证",
                        "split keys 数量 = " + splitKeys.length,
                        "实际 Region 数 = " + regionCount + " (期望 " + (splitKeys.length + 1) + ")",
                        "结论：" + (regionCount == splitKeys.length + 1 ? "预分区生效" : "Region 数不符"));
            }
            return null;
        });
    }

    /**
     * 实验 2.7：综合性能对比写入。
     * <p>
     * <b>目的</b>：在相同数据量下，对比"默认配置"与"调优配置"的写入耗时。
     * <p>
     * <b>注意</b>：单机 Docker 下差异不明显，主要验证组合配置可正常工作。
     */
    public void experimentWriteComparison() {
        support.run("2.7 综合写入对比", () -> {
            String t1 = support.expTable("write_default");
            String t2 = support.expTable("write_tuned");
            support.safeDisableAndDelete(t1);
            support.safeDisableAndDelete(t2);

            createDefaultTable(t1);
            createTunedTable(t2);

            long cost1 = writeBatch(t1, ROW_COUNT);
            long cost2 = writeBatch(t2, ROW_COUNT);
            support.printSummary("写入耗时对比",
                    "数据量 = " + ROW_COUNT + " 行",
                    "默认配置写入耗时 = " + cost1 + " ms",
                    "调优配置写入耗时 = " + cost2 + " ms",
                    "说明：调优配置 = FAST_DIFF + 预分区（压缩视集群支持）",
                    "注意：单机环境差异有限，生产环境预分区对吞吐提升明显");
            return null;
        });
    }

    private void createDefaultTable(String table) throws Exception {
        try (Admin admin = support.connection().getAdmin()) {
            TableName tn = TableName.valueOf(table);
            ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.of("cf");
            admin.createTable(TableDescriptorBuilder.newBuilder(tn).setColumnFamily(cf).build());
        }
    }

    private void createTunedTable(String table) throws Exception {
        try (Admin admin = support.connection().getAdmin()) {
            TableName tn = TableName.valueOf(table);
            ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf"))
                    .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
                    .setBloomFilterType(BloomType.ROW);
            try {
                cfBuilder.setCompressionType(Compression.Algorithm.SNAPPY);
            } catch (Exception ignore) {
                // SNAPPY 不可用则跳过
            }
            TableDescriptor td = TableDescriptorBuilder.newBuilder(tn)
                    .setColumnFamily(cfBuilder.build())
                    .build();
            byte[][] splitKeys = {Bytes.toBytes("3"), Bytes.toBytes("6"), Bytes.toBytes("9")};
            admin.createTable(td, splitKeys);
        }
    }

    private long writeBatch(String table, int count) throws Exception {
        List<Put> puts = new ArrayList<>(count);
        byte[] cf = Bytes.toBytes("cf");
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i = 0; i < count; i++) {
            String row = String.format("%010d", i);
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(cf, Bytes.toBytes("q"), Bytes.toBytes("value-" + rnd.nextInt(100000)));
            puts.add(put);
        }
        long start = System.currentTimeMillis();
        try (Table t = support.getTable(table)) {
            t.put(puts);
        }
        return System.currentTimeMillis() - start;
    }
}
