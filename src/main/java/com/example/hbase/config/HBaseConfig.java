package com.example.hbase.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class HBaseConfig {

    private static final Logger log = LoggerFactory.getLogger(HBaseConfig.class);

    /**
     * Windows 环境设置 hadoop.home.dir，避免缺少 winutils 导致启动异常。
     */
    static {
        setupHadoopHome();
    }

    private static void setupHadoopHome() {
        String hadoopHome = System.getenv("HADOOP_HOME");
        if (hadoopHome == null) {
            String projectDir = System.getProperty("user.dir");
            String winutilsDir = projectDir + "/hadoop_home";
            System.setProperty("hadoop.home.dir", winutilsDir);
            log.info("HADOOP_HOME not set, using: {}", winutilsDir);
        } else {
            System.setProperty("hadoop.home.dir", hadoopHome);
        }
    }

    @Value("${hbase.zookeeper.quorum}")
    private String zookeeperQuorum;

    @Value("${hbase.zookeeper.port}")
    private String zookeeperPort;

    @Value("${hbase.zookeeper.znode.parent}")
    private String znodeParent;

    @Value("${hbase.client.retries:3}")
    private int retries;

    @Value("${hbase.client.operation-timeout:10000}")
    private int operationTimeout;

    @Value("${hbase.client.rpc-timeout:60000}")
    private int rpcTimeout;

    @Value("${hbase.client.scanner-caching:100}")
    private int scannerCaching;

    @Bean
    public org.apache.hadoop.conf.Configuration hbaseConfiguration() {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        conf.set("hbase.zookeeper.property.clientPort", zookeeperPort);
        conf.set("zookeeper.znode.parent", znodeParent);
        conf.set("hbase.client.retries.number", String.valueOf(retries));
        conf.set("hbase.client.operation.timeout", String.valueOf(operationTimeout));
        conf.set("hbase.rpc.timeout", String.valueOf(rpcTimeout));
        conf.set("hbase.client.scanner.caching", String.valueOf(scannerCaching));
        return conf;
    }

    @Bean(destroyMethod = "close")
    public Connection hbaseConnection(org.apache.hadoop.conf.Configuration configuration) {
        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            log.info("HBase connection created successfully, zk quorum: {}", zookeeperQuorum);
            return connection;
        } catch (IOException e) {
            log.error("Failed to create HBase connection", e);
            throw new RuntimeException("HBase connection initialization failed", e);
        }
    }
}
