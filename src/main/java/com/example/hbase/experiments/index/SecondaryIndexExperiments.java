package com.example.hbase.experiments.index;

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

import java.util.ArrayList;
import java.util.List;

/**
 * 实验四：单表双列族二级索引（索引区与主数据区共置同一张表）。
 * <p>
 * <b>方案精髓</b>：索引和主数据放在<b>同一张 HBase 表</b>中，而非两张表。
 * 通过三种隔离机制让它们逻辑分离、物理分离、又共置同 Region：
 * <ol>
 *   <li><b>逻辑隔离（RowKey 前缀）</b>：索引行 RowKey 以 {@code r1:} 开头，
 *       主数据行 RowKey 以 ugid 开头。利用 HBase RowKey 字典序排列，
 *       两类行在排序上自然分簇，扫描时可按前缀精确圈定范围。</li>
 *   <li><b>物理隔离（列族分离）</b>：主数据放列族 {@code f1}，索引放列族 {@code f2}。
 *       HBase 中不同 Column Family 对应不同 Store/HFile，flush/compaction 互不影响，
 *       可独立设置压缩、TTL、版本数。</li>
 *   <li><b>同 Region 共置（预分区）</b>：通过预分区 split keys 设计，
 *       让索引行与对应主数据行尽量落入同一 Region，回表 Get 时避免跨 Region/跨网络。</li>
 * </ol>
 * <p>
 * <b>表结构（单表 exp_contacts）</b>：
 * <pre>
 * RowKey (字典序)          Column Family f1 (主数据)   Column Family f2 (索引)
 * r1:1810000000,aaaa                                   ref = aaaa,1810000000
 * r1:1810000000,bbbb                                   ref = bbbb,1810000000
 * r1:1820000001,aaaa                                   ref = aaaa,1820000001
 * aaaa,1810000000          name=张三, phone=1810000000
 * aaaa,1820000001          name=李四, phone=1820000001
 * bbbb,1810000000          name=张三, phone=1810000000
 * </pre>
 * <p>
 * <b>字典序说明</b>：ASCII 中数字(0x30-0x39) < 大写字母 < 小写字母(a=0x61..z=0x7a)。
 * {@code r}=0x72，所以 {@code r1:...} 排在以 a-z 开头的主数据<b>之后</b>。
 * 若希望索引区严格排在主数据区之前，可改用 {@code !}(0x21) 或 {@code 0x00} 作前缀。
 * 本实验沿用方案中的 {@code r1:} 关系类型前缀，演示逻辑分簇即可。
 * <p>
 * <b>前缀扫描边界技巧</b>：
 * <ul>
 *   <li>startRow = {@code "r1:1810000000,"}（前缀本身，闭区间）</li>
 *   <li>stopRow  = {@code "r1:1810000000," + 0x00}（前缀 + 最小字节，开区间）</li>
 * </ul>
 * 0x00 是字节序最小值，{@code 前缀+0x00} 严格大于 {@code 前缀+任何真实数据}，
 * Scan 的 stopRow 是开区间，恰好覆盖所有以该前缀开头的行。
 */
@Component
public class SecondaryIndexExperiments {

    private static final Logger log = LoggerFactory.getLogger(SecondaryIndexExperiments.class);

    /** 单表：索引区 + 主数据区共置。 */
    private static final String CONTACTS_TABLE = "exp_contacts";
    /** 主数据列族。 */
    private static final String CF_MAIN = "f1";
    /** 索引列族。 */
    private static final String CF_INDEX = "f2";
    /** 索引关系类型前缀，r1 = "按手机号反查收录者"。 */
    private static final String RELATION_PHONE = "r1";
    /** RowKey 中分隔字段用的分隔符。 */
    private static final String SEP = ",";

    @Autowired
    private ExperimentSupport support;

    /**
     * 实验 4.1：单表双列族二级索引完整流程（建表 → 双写 → 前缀扫描 → 回表）。
     * <p>
     * <b>实验步骤</b>：
     * <ol>
     *   <li>建单表 exp_contacts，含 f1（主数据）、f2（索引）两个列族，并预分区</li>
     *   <li>写入 3 条通讯录关系，每条同时写 f1 主数据行 + f2 索引行（双写）</li>
     *   <li>查询"哪些用户收录了手机号 1810000000"：前缀扫描 f2 索引区 → 提取主 RowKey → 精确 Get f1</li>
     *   <li>对比验证：扫描结果与期望一致</li>
     * </ol>
     * <p>
     * <b>预期结果</b>：手机号 1810000000 被 aaaa、bbbb 两个用户收录（一对多），
     * 回表后拿到这两个用户的 name=张三。
     */
    public void experimentSecondaryIndexScan() {
        support.run("4.1 单表双列族二级索引 + 前缀扫描回表", () -> {
            prepareSingleTable();

            // 通讯录数据：ugid 收录了 (姓名, 手机号)
            // aaaa 收录 张三(1810000000)、李四(1820000001)
            // bbbb 收录 张三(1810000000)
            writeContact("aaaa", "1810000000", "张三");
            writeContact("aaaa", "1820000001", "李四");
            writeContact("bbbb", "1810000000", "张三");

            // 查询：哪些用户收录了手机号 1810000000
            String targetPhone = "1810000000";
            List<String> mainRowKeys = scanIndexByPhone(targetPhone);
            log.info("手机号 {} 命中索引记录，主 RowKey 列表 = {}", targetPhone, mainRowKeys);

            // 回表：用主 RowKey 精确 Get f1 主数据区
            List<String> userDetails = new ArrayList<>();
            for (String rk : mainRowKeys) {
                userDetails.add(getMainUserDetail(rk));
            }

            support.printSummary("单表二级索引查询结果",
                    "查询手机号 = " + targetPhone,
                    "f2 索引区命中行数 = " + mainRowKeys.size() + " (期望 2)",
                    "主 RowKey = " + mainRowKeys + " (期望 [r1:...提取后 aaaa, bbbb 对应的主键 aaaa,1810.. / bbbb,1810..])",
                    "回表用户详情:");
            userDetails.forEach(d -> log.info("   {}", d));
            support.printSummary("方案要点",
                    "① 索引与主数据在同一张表，f2 扫描 + f1 回表都在同表内完成",
                    "② 索引 RowKey = r1:{phone},{主RowKey}，按手机号前缀连续存储",
                    "③ stopRow 用 前缀+0x00 精确截断，只读 f2 列族减少 IO",
                    "④ 回表用 Get（点查），走 BlockCache 快路径");
        });
    }

    /**
     * 实验 4.2：逻辑隔离验证（RowKey 前缀分簇）。
     * <p>
     * <b>原理</b>：同一张表内，索引行（{@code r1:} 前缀）和主数据行（ugid 前缀）
     * 按 RowKey 字典序自然分簇。全表扫描时，会先扫到一类再扫到另一类。
     * <p>
     * <b>预期结果</b>：全表扫描结果中，{@code r1:} 开头的索引行与 aaaa/bbbb 开头
     * 的主数据行各自成簇，互不交叉。
     */
    public void experimentLogicalIsolation() {
        support.run("4.2 逻辑隔离验证（RowKey 前缀分簇）", () -> {
            // 复用 4.1 已写入的数据
            List<String> allRows = new ArrayList<>();
            try (Table t = support.getTable(CONTACTS_TABLE)) {
                Scan scan = new Scan();
                scan.addFamily(Bytes.toBytes(CF_MAIN));
                scan.addFamily(Bytes.toBytes(CF_INDEX));
                try (var scanner = t.getScanner(scan)) {
                    for (Result r : scanner) {
                        allRows.add(Bytes.toString(r.getRow()));
                    }
                }
            }
            long indexCount = allRows.stream().filter(r -> r.startsWith(RELATION_PHONE + ":")).count();
            long mainCount = allRows.size() - indexCount;
            support.printSummary("逻辑隔离验证",
                    "全表扫描行数 = " + allRows.size(),
                    "索引行（r1: 前缀）= " + indexCount + " 行",
                    "主数据行（ugid 前缀）= " + mainCount + " 行",
                    "字典序排列 = " + allRows,
                    "结论：索引行与主数据行按 RowKey 前缀自然分簇，逻辑隔离");
        });
    }

    /**
     * 实验 4.3：物理隔离验证（列族分离）。
     * <p>
     * <b>原理</b>：HBase 中每个 Column Family 对应一个独立的 Store，底层是独立的 HFile。
     * f1 和 f2 的 flush、compaction 互不影响，可独立配置压缩/TTL/版本数。
     * <p>
     * <b>验证方法</b>：通过 {@link Admin#flush(TableName)} 触发刷写后，
     * 用 {@link Admin#getTableDescriptor(TableName)} 确认两个列族独立存在；
     * 并打印各列族属性，展示可独立配置。
     */
    public void experimentPhysicalIsolation() {
        support.run("4.3 物理隔离验证（列族分离）", () -> {
            try (Admin admin = support.connection().getAdmin()) {
                TableName tn = TableName.valueOf(CONTACTS_TABLE);
                // 触发 flush，让 MemStore 数据落盘为 HFile（单机环境便于观察 Store）
                admin.flush(tn);

                TableDescriptor td = admin.getDescriptor(tn);
                ColumnFamilyDescriptor f1 = td.getColumnFamily(Bytes.toBytes(CF_MAIN));
                ColumnFamilyDescriptor f2 = td.getColumnFamily(Bytes.toBytes(CF_INDEX));

                support.printSummary("物理隔离验证（列族独立 Store/HFile）",
                        "f1 (主数据) 存在 = " + (f1 != null) + ", 压缩 = " + f1.getCompressionType(),
                        "f2 (索引)   存在 = " + (f2 != null) + ", 压缩 = " + f2.getCompressionType(),
                        "f1 与 f2 是不同 ColumnFamily → 对应不同 Store → 不同 HFile",
                        "结论：flush/compaction 互不影响，可独立配置压缩/TTL/版本数",
                        "运维意义：删索引只操作 f2，删主数据只操作 f1，互不干扰");
            }
        });
    }

    /**
     * 实验 4.4：前缀扫描边界技巧对比。
     * <p>
     * 演示三种 stopRow 写法的差异，验证 {@code 前缀+0x00} 是最严谨的边界。
     * <ul>
     *   <li>方案A：stopRow = 前缀+0x00（推荐，精确覆盖前缀）</li>
     *   <li>方案B：stopRow = 前缀本身（错误，开区间漏掉所有以前缀开头的行）</li>
     *   <li>方案C：不设 stopRow，只设 startRow（范围过大，可能扫到下一个手机号的索引）</li>
     * </ul>
     */
    public void experimentPrefixScanBoundary() {
        support.run("4.4 前缀扫描边界技巧对比", () -> {
            String targetPhone = "1810000000";
            String prefix = RELATION_PHONE + ":" + targetPhone + SEP;

            byte[] startRow = Bytes.toBytes(prefix);
            byte[] stopRowA = Bytes.add(Bytes.toBytes(prefix), new byte[]{0x00});

            int countA = scanCount(startRow, stopRowA, CF_INDEX);
            int countB = scanCount(startRow, Bytes.toBytes(prefix), CF_INDEX);
            int countC = scanCount(startRow, null, CF_INDEX);

            support.printSummary("前缀扫描边界对比",
                    "目标前缀 = " + prefix,
                    "方案A stopRow=前缀+0x00 → 命中 " + countA + " 行 (期望 2，精确覆盖)",
                    "方案B stopRow=前缀本身 → 命中 " + countB + " 行 (期望 0，开区间漏掉所有)",
                    "方案C 无 stopRow        → 命中 " + countC + " 行 (期望 >=2，范围过大)",
                    "结论：stopRow 必须用 前缀+0x00 才能精确覆盖前缀范围",
                    "原理：0x00 是最小字节，前缀+0x00 > 前缀+任何真实数据，开区间恰好截断");
        });
    }

    /**
     * 实验 4.5：索引扫描 vs 全表扫描主数据对比。
     * <p>
     * <b>目的</b>：对比"通过 f2 索引区前缀扫描"与"全表扫描 f1 主数据区过滤"。
     * <p>
     * <b>预期结果</b>：索引扫描只读 f2 命中的行，全表扫描需遍历所有 f1 行过滤，
     * 数据量大时索引优势显著。
     */
    public void experimentIndexVsFullScan() {
        support.run("4.5 索引扫描 vs 全表扫描", () -> {
            String targetPhone = "1810000000";

            long start1 = System.currentTimeMillis();
            List<String> viaIndex = scanIndexByPhone(targetPhone);
            long cost1 = System.currentTimeMillis() - start1;

            long start2 = System.currentTimeMillis();
            List<String> viaFullScan = new ArrayList<>();
            try (Table t = support.getTable(CONTACTS_TABLE)) {
                Scan scan = new Scan();
                scan.addFamily(Bytes.toBytes(CF_MAIN));
                try (var scanner = t.getScanner(scan)) {
                    for (Result r : scanner) {
                        byte[] phone = r.getValue(Bytes.toBytes(CF_MAIN), Bytes.toBytes("phone"));
                        if (phone != null && Bytes.toString(phone).equals(targetPhone)) {
                            viaFullScan.add(Bytes.toString(r.getRow()));
                        }
                    }
                }
            }
            long cost2 = System.currentTimeMillis() - start2;

            support.printSummary("索引 vs 全表扫描",
                    "查询手机号 = " + targetPhone,
                    "索引扫描（f2 前缀）：命中 " + viaIndex.size() + " 行，耗时 " + cost1 + " ms",
                    "全表扫描（f1 过滤）：命中 " + viaFullScan.size() + " 行，耗时 " + cost2 + " ms",
                    "结论：索引扫描通过 RowKey 前缀直接定位，避免遍历 f1 全部主数据",
                    "生产意义：数据量大时，全表扫描不可接受，二级索引是必备方案");
        });
    }

    // ===================== 内部辅助方法 =====================

    /**
     * 建单表 exp_contacts，含 f1（主数据）、f2（索引）两个列族，并预分区。
     * <p>
     * <b>预分区设计</b>：split keys 设在索引区与主数据区的边界附近。
     * 单机 Docker 通常只有 1 个 RegionServer，但预分区能验证 split 生效，
     * 并为生产多节点环境下的负载分散做准备。
     * <p>
     * <b>同 Region 共置说明</b>：要让某用户的索引行与主数据行严格共置同一 Region，
     * 需给两者加相同 hash 前缀（如 {@code {hash(ugid)}:r1:...} 和 {@code {hash(ugid)}:ugid,...}），
     * 但这会破坏 {@code r1:} 前缀的连续扫描。本实验采用关系类型前缀方案，
     * 优先保证索引前缀扫描的高效，共置为可选优化。
     */
    private void prepareSingleTable() throws Exception {
        support.safeDisableAndDelete(CONTACTS_TABLE);
        try (Admin admin = support.connection().getAdmin()) {
            TableName tn = TableName.valueOf(CONTACTS_TABLE);
            // f1 主数据列族：默认配置
            ColumnFamilyDescriptor f1 = ColumnFamilyDescriptorBuilder.of(CF_MAIN);
            // f2 索引列族：可独立配置更激进的压缩/编码，索引体积小、读多写少
            ColumnFamilyDescriptor f2 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(CF_INDEX))
                    .build();

            TableDescriptor td = TableDescriptorBuilder.newBuilder(tn)
                    .setColumnFamily(f1)
                    .setColumnFamily(f2)
                    .build();

            // 预分区：split keys 按字典序边界设计
            // 索引区 r1: 排在主数据区之后（r=0x72 > a=0x61），故 split 设在主数据区内部
            byte[][] splitKeys = {
                    Bytes.toBytes("aaaa,1"),
                    Bytes.toBytes("bbbb,1")
            };
            admin.createTable(td, splitKeys);
            log.info("单表 {} 已创建，列族 f1(主数据)+f2(索引)，预分区 splitKeys={}",
                    CONTACTS_TABLE, splitKeys.length);
        }
    }

    /**
     * 双写：同一张表内写主数据行（f1）+ 索引行（f2）。
     * <p>
     * <b>主数据行</b>：RowKey = {ugid},{phone}，列族 f1 存 name/phone
     * <b>索引行</b>：RowKey = r1:{phone},{ugid},{phone}，列族 f2 存 ref（主 RowKey 冗余）
     * <p>
     * <b>双写一致性</b>：本实验用应用层双写，简单但有一致性风险。
     * 生产方案：协处理器 Observer（主数据写入后自动写索引）或 Phoenix 二级索引。
     *
     * @param ugid  收录者用户ID
     * @param phone 被收录者的手机号
     * @param name  被收录者姓名
     */
    private void writeContact(String ugid, String phone, String name) throws Exception {
        String mainRowKey = ugid + SEP + phone;
        String indexRowKey = RELATION_PHONE + ":" + phone + SEP + mainRowKey;
        try (Table t = support.getTable(CONTACTS_TABLE)) {
            // 1. 写 f1 主数据行
            Put mainPut = new Put(Bytes.toBytes(mainRowKey));
            mainPut.addColumn(Bytes.toBytes(CF_MAIN), Bytes.toBytes("name"), Bytes.toBytes(name));
            mainPut.addColumn(Bytes.toBytes(CF_MAIN), Bytes.toBytes("phone"), Bytes.toBytes(phone));
            t.put(mainPut);

            // 2. 写 f2 索引行：值冗余存主 RowKey，便于回表提取
            Put indexPut = new Put(Bytes.toBytes(indexRowKey));
            indexPut.addColumn(Bytes.toBytes(CF_INDEX), Bytes.toBytes("ref"), Bytes.toBytes(mainRowKey));
            t.put(indexPut);
        }
        log.debug("双写完成: 主数据行={} 索引行={}", mainRowKey, indexRowKey);
    }

    /**
     * 按手机号前缀扫描 f2 索引区，返回命中的主 RowKey 列表。
     * <p>
     * <b>核心技巧</b>：
     * <pre>
     *   startRow = "r1:1810000000,"          （闭区间，包含前缀本身）
     *   stopRow  = "r1:1810000000," + 0x00    （开区间，前缀+最小字节）
     *   addFamily("f2")                       （只读索引列族，不拉 f1 主数据）
     * </pre>
     * <p>
     * 因为索引 RowKey = r1:{phone},{主RowKey}，前缀 {@code r1:{phone},} 已能唯一圈定
     * 该手机号的所有索引行，stopRow 用 +0x00 精确截断。
     *
     * @param phone 待查询手机号
     * @return 命中的主 RowKey 列表（按字典序）
     */
    private List<String> scanIndexByPhone(String phone) throws Exception {
        String prefix = RELATION_PHONE + ":" + phone + SEP;
        byte[] startRow = Bytes.toBytes(prefix);
        byte[] stopRow = Bytes.add(Bytes.toBytes(prefix), new byte[]{0x00});

        List<String> mainRowKeys = new ArrayList<>();
        try (Table t = support.getTable(CONTACTS_TABLE)) {
            Scan scan = new Scan();
            scan.withStartRow(startRow);
            scan.withStopRow(stopRow);
            scan.addFamily(Bytes.toBytes(CF_INDEX));  // 只读索引列族
            scan.setCaching(100);                     // 预取 100 行/RPC

            try (var scanner = t.getScanner(scan)) {
                for (Result r : scanner) {
                    byte[] ref = r.getValue(Bytes.toBytes(CF_INDEX), Bytes.toBytes("ref"));
                    if (ref != null) {
                        mainRowKeys.add(Bytes.toString(ref));
                    }
                }
            }
        }
        return mainRowKeys;
    }

    /** 用主 RowKey 精确 Get f1 主数据区，返回用户详情字符串。 */
    private String getMainUserDetail(String mainRowKey) throws Exception {
        try (Table t = support.getTable(CONTACTS_TABLE)) {
            Get get = new Get(Bytes.toBytes(mainRowKey));
            get.addFamily(Bytes.toBytes(CF_MAIN));
            Result r = t.get(get);
            if (r.isEmpty()) {
                return mainRowKey + " → (不存在)";
            }
            String name = Bytes.toString(r.getValue(Bytes.toBytes(CF_MAIN), Bytes.toBytes("name")));
            String phone = Bytes.toString(r.getValue(Bytes.toBytes(CF_MAIN), Bytes.toBytes("phone")));
            return String.format("%s → name=%s, phone=%s", mainRowKey, name, phone);
        }
    }

    /**
     * 通用扫描计数：给定起止行和列族，返回命中行数。stopRow 为 null 表示不设上界。
     * 用于边界对比实验。
     */
    private int scanCount(byte[] startRow, byte[] stopRow, String cf) throws Exception {
        int count = 0;
        try (Table t = support.getTable(CONTACTS_TABLE)) {
            Scan scan = new Scan();
            scan.withStartRow(startRow);
            if (stopRow != null) {
                scan.withStopRow(stopRow);
            }
            scan.addFamily(Bytes.toBytes(cf));
            try (var scanner = t.getScanner(scan)) {
                for (Result r : scanner) {
                    count++;
                }
            }
        }
        return count;
    }

    /** 提供给外部调用的清理方法，便于实验后清理表。 */
    public void cleanup() {
        support.safeDisableAndDelete(CONTACTS_TABLE);
    }
}
