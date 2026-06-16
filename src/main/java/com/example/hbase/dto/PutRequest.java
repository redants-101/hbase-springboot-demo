package com.example.hbase.dto;

import lombok.Data;

import javax.validation.constraints.NotBlank;


@Data
public class PutRequest {
    @NotBlank
    private String tableName;
    @NotBlank
    private String rowKey;
    @NotBlank
    private String columnFamily;
    @NotBlank
    private String qualifier;
    private String value;
}