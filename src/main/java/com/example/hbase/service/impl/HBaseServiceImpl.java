package com.example.hbase.service.impl;

import com.example.hbase.exception.BusinessException;
import com.example.hbase.service.HBaseService;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HBaseServiceImpl implements HBaseService {

    private static final Logger log = LoggerFactory.getLogger(HBaseServiceImpl.class);

    @Autowired
    private Connection connection;

    @Override
    public void createTable(String tableName, String... columnFamilies) {
        if (columnFamilies == null || columnFamilies.length == 0) {
            throw new BusinessException("At least one column family is required");
        }
        try (Admin admin = connection.getAdmin()) {
            TableName tn = TableName.valueOf(tableName);
            if (admin.tableExists(tn)) {
                log.warn("Table {} already exists, skip creation", tableName);
                return;
            }
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tn);
            for (String cf : columnFamilies) {
                builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf));
            }
            admin.createTable(builder.build());
            log.info("Table {} created successfully with families: {}", tableName, (Object) columnFamilies);
        } catch (IOException e) {
            log.error("Failed to create table {}", tableName, e);
            throw new BusinessException("Create table failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void putData(String tableName, String rowKey, String columnFamily, String qualifier, String value) {
        if (value == null) {
            // HBase不支持null值，可以考虑删除该列
            throw new BusinessException("Value cannot be null for put");
        }
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            log.debug("Put data: table={}, row={}, cf={}, qualifier={}, value={}", tableName, rowKey, columnFamily, qualifier, value);
        } catch (IOException e) {
            log.error("Failed to put data", e);
            throw new BusinessException("Put data failed: " + e.getMessage(), e);
        }
    }

    @Override
    public String getData(String tableName, String rowKey, String columnFamily, String qualifier) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            if (StringUtils.hasText(columnFamily)) {
                if (StringUtils.hasText(qualifier)) {
                    get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
                } else {
                    get.addFamily(Bytes.toBytes(columnFamily));
                }
            }
            Result result = table.get(get);
            if (result.isEmpty()) {
                return null;
            }
            if (StringUtils.hasText(columnFamily) && StringUtils.hasText(qualifier)) {
                byte[] value = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
                return value == null ? null : Bytes.toString(value);
            } else {
                // 返回整行的第一个值简化示例，实际应返回Map
                for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(Bytes.toBytes(columnFamily)).entrySet()) {
                    return Bytes.toString(entry.getValue());
                }
                return null;
            }
        } catch (IOException e) {
            log.error("Failed to get data", e);
            throw new BusinessException("Get data failed: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<String, String> getRow(String tableName, String rowKey) {
        Map<String, String> rowMap = new HashMap<>();
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            if (result.isEmpty()) {
                return rowMap;
            }
            Arrays.stream(result.rawCells()).forEach(cell -> {
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                rowMap.put(family + ":" + qualifier, value);
            });
        } catch (IOException e) {
            log.error("Failed to get row", e);
            throw new BusinessException("Get row failed: " + e.getMessage(), e);
        }
        return rowMap;
    }

    @Override
    public List<Map<String, String>> scanTable(String tableName, String startRow, String stopRow) {
        List<Map<String, String>> results = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Scan scan = new Scan();
            if (StringUtils.hasText(startRow)) {
                scan.withStartRow(Bytes.toBytes(startRow));
            }
            if (StringUtils.hasText(stopRow)) {
                scan.withStopRow(Bytes.toBytes(stopRow));
            }
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    Map<String, String> rowData = new HashMap<>();
                    rowData.put("rowKey", Bytes.toString(result.getRow()));
                    Arrays.stream(result.rawCells()).forEach(cell -> {
                        String family = Bytes.toString(CellUtil.cloneFamily(cell));
                        String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                        String value = Bytes.toString(CellUtil.cloneValue(cell));
                        rowData.put(family + ":" + qualifier, value);
                    });
                    results.add(rowData);
                }
            }
        } catch (IOException e) {
            log.error("Failed to scan table", e);
            throw new BusinessException("Scan table failed: " + e.getMessage(), e);
        }
        return results;
    }

    @Override
    public void deleteRow(String tableName, String rowKey) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            log.info("Deleted row: table={}, rowKey={}", tableName, rowKey);
        } catch (IOException e) {
            log.error("Failed to delete row", e);
            throw new BusinessException("Delete row failed: " + e.getMessage(), e);
        }
    }
}