package com.example.kafka_demo.Partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author Connor
 * @Date 2022-08-26 週五  21:34:14
 */
public class CustomPartitioner implements Partitioner {
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        String messageValue = o1.toString();

        int partition;
        if (messageValue.contains("hello")){
            partition = 0;
        } else {
            partition = 2;
        }

        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
