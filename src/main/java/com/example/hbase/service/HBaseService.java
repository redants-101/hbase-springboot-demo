package com.example.hbase.service;

import java.util.List;
import java.util.Map;

public interface HBaseService {
    void createTable(String tableName, String... columnFamilies);
    void putData(String tableName, String rowKey, String columnFamily, String qualifier, String value);
    String getData(String tableName, String rowKey, String columnFamily, String qualifier);
    Map<String, String> getRow(String tableName, String rowKey);
    List<Map<String, String>> scanTable(String tableName, String startRow, String stopRow);
    void deleteRow(String tableName, String rowKey);
}