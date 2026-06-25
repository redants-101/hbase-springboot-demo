package com.example.hbase.controller;

import com.example.hbase.dto.ApiResponse;
import com.example.hbase.experiments.advanced.AdvancedFeatureExperiments;
import com.example.hbase.experiments.index.SecondaryIndexExperiments;
import com.example.hbase.experiments.performance.PerformanceExperiments;
import com.example.hbase.experiments.table.TableDesignExperiments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * HBase 属性学习实验统一入口。
 * <p>
 * 通过 REST 接口触发各类实验，便于在 IDE 或浏览器中按需运行单个实验。
 * 所有实验日志通过 logback 输出到控制台和文件，结果摘要也以 JSON 返回。
 * <p>
 * 路由约定：
 * <ul>
 *   <li>{@code GET /api/experiment/list}         列出所有可用实验</li>
 *   <li>{@code GET /api/experiment/run/{id}}     运行指定实验</li>
 *   <li>{@code GET /api/experiment/group/{group}} 运行整组实验</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/experiment")
public class ExperimentController {

    private static final Logger log = LoggerFactory.getLogger(ExperimentController.class);

    @Autowired
    private TableDesignExperiments tableDesignExperiments;
    @Autowired
    private PerformanceExperiments performanceExperiments;
    @Autowired
    private AdvancedFeatureExperiments advancedFeatureExperiments;
    @Autowired
    private SecondaryIndexExperiments secondaryIndexExperiments;

    /** 列出所有可用实验及其所属分组。 */
    @GetMapping("/list")
    public ApiResponse<Map<String, Object>> list() {
        Map<String, Object> groups = new LinkedHashMap<>();
        Map<String, String> table = new LinkedHashMap<>();
        table.put("1.1", "列族属性读取与对比");
        table.put("1.2", "多版本控制 MAX_VERSIONS");
        table.put("1.3", "TTL 数据过期");
        table.put("1.4", "MIN_VERSIONS 与 TTL 配合");
        table.put("1.5", "KEEP_DELETED_CELLS 时间旅行");
        groups.put("table", table);

        Map<String, String> perf = new LinkedHashMap<>();
        perf.put("2.1", "布隆过滤器 BloomFilter");
        perf.put("2.2", "块大小 BLOCKSIZE");
        perf.put("2.3", "压缩算法 Compression");
        perf.put("2.4", "数据块编码 DataBlockEncoding");
        perf.put("2.5", "块缓存 IN_MEMORY");
        perf.put("2.6", "预分区 Pre-Split");
        perf.put("2.7", "综合写入对比");
        groups.put("performance", perf);

        Map<String, String> adv = new LinkedHashMap<>();
        adv.put("3.1", "过滤器组合");
        adv.put("3.2", "批量操作 batch");
        adv.put("3.3", "计数器 Increment");
        adv.put("3.4", "快照 Snapshot");
        adv.put("3.5", "异步客户端 AsyncConnection");
        groups.put("advanced", adv);

        Map<String, String> idx = new LinkedHashMap<>();
        idx.put("4.1", "单表双列族二级索引 + 前缀扫描回表");
        idx.put("4.2", "逻辑隔离验证（RowKey 前缀分簇）");
        idx.put("4.3", "物理隔离验证（列族分离）");
        idx.put("4.4", "前缀扫描边界技巧对比");
        idx.put("4.5", "索引扫描 vs 全表扫描");
        groups.put("index", idx);

        return ApiResponse.success(groups);
    }

    /** 运行单个实验。 */
    @GetMapping("/run/{id}")
    public ApiResponse<String> run(@PathVariable String id) {
        log.info("收到运行实验请求: id={}", id);
        try {
            switch (id) {
                case "1.1": tableDesignExperiments.experimentColumnFamilyAttributes(); break;
                case "1.2": tableDesignExperiments.experimentMaxVersions(); break;
                case "1.3": tableDesignExperiments.experimentTTL(); break;
                case "1.4": tableDesignExperiments.experimentMinVersions(); break;
                case "1.5": tableDesignExperiments.experimentKeepDeletedCells(); break;

                case "2.1": performanceExperiments.experimentBloomFilter(); break;
                case "2.2": performanceExperiments.experimentBlockSize(); break;
                case "2.3": performanceExperiments.experimentCompression(); break;
                case "2.4": performanceExperiments.experimentDataBlockEncoding(); break;
                case "2.5": performanceExperiments.experimentBlockCache(); break;
                case "2.6": performanceExperiments.experimentPreSplit(); break;
                case "2.7": performanceExperiments.experimentWriteComparison(); break;

                case "3.1": advancedFeatureExperiments.experimentFilters(); break;
                case "3.2": advancedFeatureExperiments.experimentBatch(); break;
                case "3.3": advancedFeatureExperiments.experimentCounter(); break;
                case "3.4": advancedFeatureExperiments.experimentSnapshot(); break;
                case "3.5": advancedFeatureExperiments.experimentAsyncClient(); break;

                case "4.1": secondaryIndexExperiments.experimentSecondaryIndexScan(); break;
                case "4.2": secondaryIndexExperiments.experimentLogicalIsolation(); break;
                case "4.3": secondaryIndexExperiments.experimentPhysicalIsolation(); break;
                case "4.4": secondaryIndexExperiments.experimentPrefixScanBoundary(); break;
                case "4.5": secondaryIndexExperiments.experimentIndexVsFullScan(); break;

                default:
                    return ApiResponse.error("未知实验 ID: " + id + "，请调用 /api/experiment/list 查看");
            }
            return ApiResponse.success("实验 " + id + " 执行完成，详情查看日志");
        } catch (Exception e) {
            log.error("实验 {} 执行失败", id, e);
            return ApiResponse.error("实验执行失败: " + e.getMessage());
        }
    }

    /** 运行整组实验。 */
    @GetMapping("/group/{group}")
    public ApiResponse<String> runGroup(@PathVariable String group) {
        log.info("收到运行实验组请求: group={}", group);
        try {
            switch (group) {
                case "table":
                    tableDesignExperiments.experimentColumnFamilyAttributes();
                    tableDesignExperiments.experimentMaxVersions();
                    tableDesignExperiments.experimentTTL();
                    tableDesignExperiments.experimentMinVersions();
                    tableDesignExperiments.experimentKeepDeletedCells();
                    break;
                case "performance":
                    performanceExperiments.experimentBloomFilter();
                    performanceExperiments.experimentBlockSize();
                    performanceExperiments.experimentCompression();
                    performanceExperiments.experimentDataBlockEncoding();
                    performanceExperiments.experimentBlockCache();
                    performanceExperiments.experimentPreSplit();
                    performanceExperiments.experimentWriteComparison();
                    break;
                case "advanced":
                    advancedFeatureExperiments.experimentFilters();
                    advancedFeatureExperiments.experimentBatch();
                    advancedFeatureExperiments.experimentCounter();
                    advancedFeatureExperiments.experimentSnapshot();
                    advancedFeatureExperiments.experimentAsyncClient();
                    break;
                case "index":
                    secondaryIndexExperiments.experimentSecondaryIndexScan();
                    secondaryIndexExperiments.experimentLogicalIsolation();
                    secondaryIndexExperiments.experimentPhysicalIsolation();
                    secondaryIndexExperiments.experimentPrefixScanBoundary();
                    secondaryIndexExperiments.experimentIndexVsFullScan();
                    break;
                default:
                    return ApiResponse.error("未知实验组: " + group + "，可选: table/performance/advanced/index");
            }
            return ApiResponse.success("实验组 " + group + " 执行完成，详情查看日志");
        } catch (Exception e) {
            log.error("实验组 {} 执行失败", group, e);
            return ApiResponse.error("实验组执行失败: " + e.getMessage());
        }
    }

    /** 清理二级索引实验产生的表。 */
    @GetMapping("/cleanup/index")
    public ApiResponse<String> cleanupIndex() {
        secondaryIndexExperiments.cleanup();
        return ApiResponse.success("二级索引实验表已清理");
    }
}
