package com.example.hbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class HbaseSpringbootApplication {
    public static void main(String[] args) {
        // Windows环境下设置HADOOP_HOME，避免winutils.exe缺失导致异常
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            String hadoopHome = System.getenv("HADOOP_HOME");
            if (hadoopHome == null) {
                String userDir = System.getProperty("user.dir");
                System.setProperty("hadoop.home.dir", userDir + "/hadoop_home");
            } else {
                System.setProperty("hadoop.home.dir", hadoopHome);
            }
        }
        SpringApplication.run(HbaseSpringbootApplication.class, args);
    }
}