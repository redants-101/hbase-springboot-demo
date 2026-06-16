package com.example.hbase;

import com.example.hbase.service.HBaseService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 集成测试，需要真实可访问的 HBase 集群。
 */
@SpringBootTest
public class HBaseServiceIT {

    @Autowired
    private HBaseService hBaseService;

    @Test
    public void testCreateAndPutAndGet() {
        String table = "test_springboot";
        String cf = "cf1";
        String rowKey = "row1";
        String qualifier = "name";
        String value = "SpringBootHBase";

        hBaseService.createTable(table, cf);
        hBaseService.putData(table, rowKey, cf, qualifier, value);

        String result = hBaseService.getData(table, rowKey, cf, qualifier);
        assertThat(result).isEqualTo(value);

        hBaseService.deleteRow(table, rowKey);
    }
}
