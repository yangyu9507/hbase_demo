package com.gy.hbase.config;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;


/**
 * created by yangyu on 2019-11-08
 */
@org.springframework.context.annotation.Configuration
@PropertySource("classpath:hbase.properties")
@Slf4j
public class HBaseConfig {

    @Value(value = "${hbase.rootdir}")
    private String rootdir;

    @Value(value = "${hbase.cluster.distributed}")
    private String clusterDistributed;

    @Value(value = "${hbase.zookeeper.quorum}")
    private String zookeeperQuorum;

    @Value(value = "${hbase.zookeeper.property.clientPort}")
    private String zookeeperPropertyClientPort;

    @Value(value = "${hbase.master}")
    private String master;

    @Bean
    public Connection getConn() {
        Connection connection = null;
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", rootdir);
        configuration.set("hbase.cluster.distributed", clusterDistributed);
        configuration.set("hbase.master", master);
        configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
        configuration.set("hbase.zookeeper.property.clientPort", zookeeperPropertyClientPort);
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException ex) {
            log.error("Get connection to hbase failed : ", ex);
        }
        return connection;
    }

}
