package com.example.hbase;

import com.example.hbase.service.HBaseService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
public class HBaseServiceTest {

    @Autowired
    private HBaseService hBaseService;

    @Test
    public void testCreateAndPutAndGet() {
        String table = "test_springboot";
        String cf = "cf1";
        String rowKey = "row1";
        String qualifier = "name";
        String value = "SpringBootHBase";

        // 创建表
        hBaseService.createTable(table, cf);

        // 插入数据
        hBaseService.putData(table, rowKey, cf, qualifier, value);

        // 读取数据
        String result = hBaseService.getData(table, rowKey, cf, qualifier);
        assertThat(result).isEqualTo(value);

        // 清理
        hBaseService.deleteRow(table, rowKey);
    }
}