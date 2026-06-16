package com.example.hbase.dto;

import lombok.Data;

import javax.validation.constraints.NotBlank;

@Data
public class GetRequest {
    @NotBlank
    private String tableName;
    @NotBlank
    private String rowKey;
    private String columnFamily;
    private String qualifier;
}