package com.example.kafka_demo.producerTest;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Properties;

/**
 * @author Connor
 * @Date 2022-08-26 週五  22:33:28
 */
@SpringBootTest
public class TransactionTest {

    @Test
    public void transaction() {
        //設定
        Properties properties = new Properties();
        //連接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "K4:9092");
        //設定序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //開啟Transaction 一定要配置transaction_id
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tran_1");

        //產生一個producer實體
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);
        kafkaProducer.initTransactions();

        kafkaProducer.beginTransaction();

        try {
            //send訊息
            for (int i = 1; i <= 10; i++) {
                kafkaProducer.send(new ProducerRecord<>("first", "hello " + i), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            System.out.println("Topic : " + recordMetadata.topic() + ", Partition : " + recordMetadata.partition());
                        }
                    }
                });
            }
            kafkaProducer.commitTransaction();
        } catch (Exception e) {
            kafkaProducer.abortTransaction();
        } finally {
            //關閉
            kafkaProducer.close();
        }
    }
}
