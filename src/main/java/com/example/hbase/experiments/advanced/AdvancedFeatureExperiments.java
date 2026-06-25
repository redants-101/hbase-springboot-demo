package com.example.hbase.experiments.advanced;

import com.example.hbase.experiments.ExperimentSupport;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 实验三：高级特性（过滤器 / 批量操作 / 计数器 / 快照 / 异步客户端）。
 * <p>
 * 这些是 HBase API 层面的高级能力，与表属性配合使用可解决复杂业务场景。
 */
@Component
public class AdvancedFeatureExperiments {

    private static final Logger log = LoggerFactory.getLogger(AdvancedFeatureExperiments.class);

    @Autowired
    private ExperimentSupport support;

    /**
     * 实验 3.1：过滤器（Filter）组合使用。
     * <p>
     * <b>原理</b>：HBase 过滤器在 RegionServer 端执行，减少网络传输。
     * <ul>
     *   <li>{@link RowFilter} + {@link RegexStringComparator}：行键正则</li>
     *   <li>{@link ColumnPrefixFilter}：列前缀</li>
     *   <li>{@link SingleColumnValueFilter}：列值条件</li>
     *   <li>{@link PageFilter}：分页</li>
     *   <li>{@link FilterList}：组合多个过滤器（AND/OR）</li>
     * </ul>
     * <p>
     * <b>实验步骤</b>：写入 10 行数据（user_00~user_09），每行含 age、name 列。
     * 演示：① 行键正则 user_0[1-5] ② age>5 的行 ③ 分页 2 条 ④ 组合过滤。
     */
    public void experimentFilters() {
        support.run("3.1 过滤器组合", () -> {
            String table = support.expTable("filters");
            support.safeDisableAndDelete(table);
            try (Admin admin = support.connection().getAdmin()) {
                ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.of("cf");
                admin.createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(table))
                        .setColumnFamily(cf).build());
            }
            try (Table t = support.getTable(table)) {
                byte[] cf = Bytes.toBytes("cf");
                for (int i = 0; i < 10; i++) {
                    Put put = new Put(Bytes.toBytes("user_" + String.format("%02d", i)));
                    put.addColumn(cf, Bytes.toBytes("name"), Bytes.toBytes("name" + i));
                    put.addColumn(cf, Bytes.toBytes("age"), Bytes.toBytes(String.valueOf(i)));
                    t.put(put);
                }

                // ① 行键正则：user_01 ~ user_05
                Scan scan1 = new Scan();
                scan1.setFilter(new RowFilter(CompareOperator.EQUAL,
                        new RegexStringComparator("user_0[1-5]")));
                List<String> r1 = scanRowKeys(t, scan1);

                // ② 列值：age > 5
                Scan scan2 = new Scan();
                SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
                        cf, Bytes.toBytes("age"), CompareOperator.GREATER, Bytes.toBytes("5"));
                valueFilter.setFilterIfMissing(true);
                scan2.setFilter(valueFilter);
                List<String> r2 = scanRowKeys(t, scan2);

                // ③ 分页：每页 2 条
                Scan scan3 = new Scan();
                scan3.setFilter(new PageFilter(2));
                List<String> r3 = scanRowKeys(t, scan3);

                // ④ 组合：行键正则 AND 列前缀 na
                Scan scan4 = new Scan();
                FilterList combo = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                        new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("user_0[0-9]")),
                        new ColumnPrefixFilter(Bytes.toBytes("na")));
                scan4.setFilter(combo);
                List<String> r4 = scanRowKeys(t, scan4);

                support.printSummary("过滤器实验结果",
                        "① 行键正则 user_0[1-5] → " + r1 + " (期望 user_01..user_05)",
                        "② age>5 → " + r2 + " (期望 user_06..user_09)",
                        "③ PageFilter(2) → " + r3 + " (期望前2条 user_00,user_01)",
                        "④ 行键正则 AND 列前缀na → " + r4.size() + " 行 (期望10行，每行都有na前缀列)");
            }
            return null;
        });
    }

    /**
     * 实验 3.2：批量操作（batch）。
     * <p>
     * <b>原理</b>：{@link Table#batch(List, Object[])} 可在一次 RPC 中混合
     * Put/Get/Delete，降低往返开销。注意 batch 是部分失败的，需检查 results 数组。
     */
    public void experimentBatch() {
        support.run("3.2 批量操作 batch", () -> {
            String table = support.expTable("batch");
            support.safeDisableAndDelete(table);
            try (Admin admin = support.connection().getAdmin()) {
                admin.createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(table))
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build());
            }
            try (Table t = support.getTable(table)) {
                byte[] cf = Bytes.toBytes("cf");
                List<Put> puts = new ArrayList<>();
                for (int i = 0; i < 5; i++) {
                    puts.add(new Put(Bytes.toBytes("r" + i)).addColumn(cf, Bytes.toBytes("v"), Bytes.toBytes("val" + i)));
                }
                t.put(puts);
                log.info("批量写入 5 行完成");

                List<org.apache.hadoop.hbase.client.Row> actions = new ArrayList<>();
                actions.add(new Get(Bytes.toBytes("r0")).addColumn(cf, Bytes.toBytes("v")));
                actions.add(new Get(Bytes.toBytes("r1")).addColumn(cf, Bytes.toBytes("v")));
                actions.add(new Delete(Bytes.toBytes("r2")));
                Object[] results = new Object[actions.size()];
                t.batch(actions, results);

                String r0val = results[0] instanceof Result ? Bytes.toString(((Result) results[0]).getValue(cf, Bytes.toBytes("v"))) : null;
                String r1val = results[1] instanceof Result ? Bytes.toString(((Result) results[1]).getValue(cf, Bytes.toBytes("v"))) : null;
                Result r2check = t.get(new Get(Bytes.toBytes("r2")));

                support.printSummary("批量操作结果",
                        "batch Get r0 = " + r0val + " (期望 val0)",
                        "batch Get r1 = " + r1val + " (期望 val1)",
                        "batch Delete r2 后查询 isEmpty = " + r2check.isEmpty() + " (期望 true)",
                        "结论：batch 支持混合操作，单次 RPC 完成多种动作");
            }
            return null;
        });
    }

    /**
     * 实验 3.3：计数器（Increment）原子操作。
     * <p>
     * <b>原理</b>：HBase 支持对 Cell 做原子自增，无需读-改-写，适合计数器、点赞、PV。
     * {@link Table#increment(Increment)} 返回自增后的最新值。
     * <p>
     * <b>预期结果</b>：自增 10 次，最终值为 10，无并发丢失。
     */
    public void experimentCounter() {
        support.run("3.3 计数器 Increment", () -> {
            String table = support.expTable("counter");
            support.safeDisableAndDelete(table);
            try (Admin admin = support.connection().getAdmin()) {
                admin.createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(table))
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build());
            }
            try (Table t = support.getTable(table)) {
                byte[] row = Bytes.toBytes("counter_row");
                byte[] cf = Bytes.toBytes("cf");
                byte[] q = Bytes.toBytes("hits");

                long finalVal = 0;
                for (int i = 0; i < 10; i++) {
                    Increment inc = new Increment(row);
                    inc.addColumn(cf, q, 1L);
                    Result r = t.increment(inc);
                    finalVal = Bytes.toLong(r.getValue(cf, q));
                }
                support.printSummary("计数器实验结果",
                        "自增 10 次，每次 +1",
                        "最终值 = " + finalVal + " (期望 10)",
                        "结论：Increment 是行级原子操作，无并发丢失，适合计数场景");
            }
            return null;
        });
    }

    /**
     * 实验 3.4：快照（Snapshot）。
     * <p>
     * <b>原理</b>：快照是表在某一时刻的元数据引用，不立即复制数据，创建极快。
     * 可用于：备份、恢复（restore_snapshot）、克隆新表（clone_snapshot）。
     * <p>
     * <b>预期结果</b>：克隆表只包含快照时刻的数据，不含后续写入。
     */
    public void experimentSnapshot() {
        support.run("3.4 快照 Snapshot", () -> {
            String table = support.expTable("snapshot_src");
            String cloneTable = support.expTable("snapshot_clone");
            String snapName = "exp_snapshot_v1";
            support.safeDisableAndDelete(table);
            support.safeDisableAndDelete(cloneTable);
            try (Admin admin = support.connection().getAdmin()) {
                admin.createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(table))
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build());
            }
            try (Table t = support.getTable(table)) {
                byte[] cf = Bytes.toBytes("cf");
                t.put(new Put(Bytes.toBytes("r1")).addColumn(cf, Bytes.toBytes("v"), Bytes.toBytes("before_snapshot")));
            }
            try (Admin admin = support.connection().getAdmin()) {
                if (admin.listSnapshots().stream().anyMatch(s -> s.getName().equals(snapName))) {
                    admin.deleteSnapshot(snapName);
                }
                admin.snapshot(snapName, TableName.valueOf(table));
                log.info("已创建快照: {}", snapName);
            }
            try (Table t = support.getTable(table)) {
                byte[] cf = Bytes.toBytes("cf");
                t.put(new Put(Bytes.toBytes("r2")).addColumn(cf, Bytes.toBytes("v"), Bytes.toBytes("after_snapshot")));
            }
            try (Admin admin = support.connection().getAdmin()) {
                admin.cloneSnapshot(snapName, TableName.valueOf(cloneTable));
            }
            try (Table t = support.getTable(cloneTable)) {
                byte[] cf = Bytes.toBytes("cf");
                Result r1 = t.get(new Get(Bytes.toBytes("r1")).addColumn(cf, Bytes.toBytes("v")));
                Result r2 = t.get(new Get(Bytes.toBytes("r2")).addColumn(cf, Bytes.toBytes("v")));
                support.printSummary("快照实验结果",
                        "快照前写入 r1 在克隆表中: " + (!r1.isEmpty()) + " (期望 true)",
                        "快照后写入 r2 在克隆表中: " + (!r2.isEmpty()) + " (期望 false)",
                        "结论：快照保留建快照时刻的数据状态，克隆表不含后续写入",
                        "应用：零拷贝备份、A/B 测试、误操作回滚");
            }
            return null;
        });
    }

    /**
     * 实验 3.5：异步客户端（AsyncConnection）。
     * <p>
     * <b>原理</b>：HBase 2.x 提供 {@link AsyncConnection}，基于 CompletableFuture，
     * 无阻塞线程，适合高并发 IO 密集场景。
     * <p>
     * <b>预期结果</b>：异步操作完成，数据正确写入。
     */
    public void experimentAsyncClient() {
        support.run("3.5 异步客户端 AsyncConnection", () -> {
            String table = support.expTable("async");
            support.safeDisableAndDelete(table);
            try (Admin admin = support.connection().getAdmin()) {
                admin.createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(table))
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf")).build());
            }
            try (AsyncConnection asyncConn = ConnectionFactory.createAsyncConnection(
                    support.connection().getConfiguration()).get()) {
                AsyncTable<org.apache.hadoop.hbase.client.AdvancedScanResultConsumer> asyncTable = asyncConn.getTable(TableName.valueOf(table));
                byte[] cf = Bytes.toBytes("cf");

                List<CompletableFuture<Void>> putFutures = new ArrayList<>();
                for (int i = 0; i < 5; i++) {
                    Put put = new Put(Bytes.toBytes("ar" + i))
                            .addColumn(cf, Bytes.toBytes("v"), Bytes.toBytes("aval" + i));
                    putFutures.add(asyncTable.put(put));
                }
                CompletableFuture.allOf(putFutures.toArray(new CompletableFuture[0])).join();
                log.info("异步写入 5 行完成");

                CompletableFuture<Result> getFuture = asyncTable.get(
                        new Get(Bytes.toBytes("ar0")).addColumn(cf, Bytes.toBytes("v")));
                Result r = getFuture.join();
                String val = Bytes.toString(r.getValue(cf, Bytes.toBytes("v")));
                support.printSummary("异步客户端实验结果",
                        "异步写入 5 行后读取 ar0 = " + val + " (期望 aval0)",
                        "结论：AsyncConnection 基于 CompletableFuture，非阻塞，适合高并发",
                        "对比：同步客户端每操作占一个线程，异步客户端少量线程处理大量 IO");
            }
            return null;
        });
    }

    private List<String> scanRowKeys(Table t, Scan scan) throws Exception {
        List<String> rows = new ArrayList<>();
        try (var scanner = t.getScanner(scan)) {
            for (Result r : scanner) {
                rows.add(Bytes.toString(r.getRow()));
            }
        }
        return rows;
    }
}
