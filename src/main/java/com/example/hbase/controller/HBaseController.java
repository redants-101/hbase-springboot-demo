package com.example.hbase.controller;

import com.example.hbase.dto.ApiResponse;
import com.example.hbase.dto.PutRequest;
import com.example.hbase.service.HBaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/hbase")
@Validated
public class HBaseController {

    @Autowired
    private HBaseService hBaseService;

    @PostMapping("/table")
    public ApiResponse<Void> createTable(@RequestParam String tableName, @RequestParam String[] columnFamilies) {
        hBaseService.createTable(tableName, columnFamilies);
        return ApiResponse.success(null);
    }

    @PostMapping("/data")
    public ApiResponse<Void> putData(@Valid @RequestBody PutRequest request) {
        hBaseService.putData(request.getTableName(), request.getRowKey(),
                request.getColumnFamily(), request.getQualifier(), request.getValue());
        return ApiResponse.success(null);
    }

    @GetMapping("/data")
    public ApiResponse<String> getData(@RequestParam String tableName,
                                       @RequestParam String rowKey,
                                       @RequestParam(required = false) String columnFamily,
                                       @RequestParam(required = false) String qualifier) {
        String value = hBaseService.getData(tableName, rowKey, columnFamily, qualifier);
        return ApiResponse.success(value);
    }

    @GetMapping("/row")
    public ApiResponse<Map<String, String>> getRow(@RequestParam String tableName,
                                                   @RequestParam String rowKey) {
        Map<String, String> row = hBaseService.getRow(tableName, rowKey);
        return ApiResponse.success(row);
    }

    @GetMapping("/scan")
    public ApiResponse<List<Map<String, String>>> scanTable(@RequestParam String tableName,
                                                            @RequestParam(required = false) String startRow,
                                                            @RequestParam(required = false) String stopRow) {
        List<Map<String, String>> rows = hBaseService.scanTable(tableName, startRow, stopRow);
        return ApiResponse.success(rows);
    }

    @DeleteMapping("/row")
    public ApiResponse<Void> deleteRow(@RequestParam String tableName, @RequestParam String rowKey) {
        hBaseService.deleteRow(tableName, rowKey);
        return ApiResponse.success(null);
    }
}