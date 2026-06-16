package com.example.hbase.service.impl;

import com.example.hbase.exception.BusinessException;
import com.example.hbase.service.HBaseService;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
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
        if (columnFamilies == null || columnFamilies.length == 0
                || Arrays.stream(columnFamilies).anyMatch(cf -> !StringUtils.hasText(cf))) {
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
            throw new BusinessException("Value cannot be null for put");
        }
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            log.debug("Put data: table={}, row={}, cf={}, qualifier={}, value={}",
                    tableName, rowKey, columnFamily, qualifier, value);
        } catch (IOException e) {
            log.error("Failed to put data", e);
            throw new BusinessException("Put data failed: " + e.getMessage(), e);
        }
    }

    @Override
    public String getData(String tableName, String rowKey, String columnFamily, String qualifier) {
        if (!StringUtils.hasText(columnFamily) || !StringUtils.hasText(qualifier)) {
            throw new BusinessException("columnFamily and qualifier are required for single-cell reads");
        }
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
            Result result = table.get(get);
            if (result.isEmpty()) {
                return null;
            }
            byte[] value = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
            return value == null ? null : Bytes.toString(value);
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
