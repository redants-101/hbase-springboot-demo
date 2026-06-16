package com.example.hbase.controller;

import com.example.hbase.dto.ApiResponse;
import com.example.hbase.dto.PutRequest;
import com.example.hbase.service.HBaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
    public ApiResponse<Void> createTable(@RequestParam @NotBlank String tableName,
                                         @RequestParam String[] columnFamilies) {
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
    public ApiResponse<String> getData(@RequestParam @NotBlank String tableName,
                                       @RequestParam @NotBlank String rowKey,
                                       @RequestParam @NotBlank String columnFamily,
                                       @RequestParam @NotBlank String qualifier) {
        String value = hBaseService.getData(tableName, rowKey, columnFamily, qualifier);
        return ApiResponse.success(value);
    }

    @GetMapping("/row")
    public ApiResponse<Map<String, String>> getRow(@RequestParam @NotBlank String tableName,
                                                   @RequestParam @NotBlank String rowKey) {
        Map<String, String> row = hBaseService.getRow(tableName, rowKey);
        return ApiResponse.success(row);
    }

    @GetMapping("/scan")
    public ApiResponse<List<Map<String, String>>> scanTable(@RequestParam @NotBlank String tableName,
                                                            @RequestParam(required = false) String startRow,
                                                            @RequestParam(required = false) String stopRow) {
        List<Map<String, String>> rows = hBaseService.scanTable(tableName, startRow, stopRow);
        return ApiResponse.success(rows);
    }

    @DeleteMapping("/row")
    public ApiResponse<Void> deleteRow(@RequestParam @NotBlank String tableName,
                                       @RequestParam @NotBlank String rowKey) {
        hBaseService.deleteRow(tableName, rowKey);
        return ApiResponse.success(null);
    }
}
