package com.example.kafka_demo.producerTest;

import com.example.kafka_demo.Partitioner.CustomPartitioner;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Optional;
import java.util.Properties;

/**
 * @author Connor
 * @Date 2022-08-26 週五  17:36:32
 */
@SpringBootTest
class CustomProducer {

    @Test
    public void producer() {
        //設定
        Properties properties = new Properties();
        //連接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "K4:9092");
        //設定序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //產生一個producer實體
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);

        try {
            //send訊息
            for (int i = 1; i <= 10; i++) {
                kafkaProducer.send(new ProducerRecord<>("first", "hello " + i));
            }
        } finally {
            //關閉
            kafkaProducer.close();
        }

    }

    @Test
    public void producerCallBack() {
        //設定
        Properties properties = new Properties();
        //連接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "K4:9092");
        //設定序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //產生一個producer實體
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);

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
        } finally {
            //關閉
            kafkaProducer.close();
        }
    }

    @Test
    public void producerCallBackPartition() {
        //設定
        Properties properties = new Properties();
        //連接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "K4:9092");
        //設定序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //產生一個producer實體
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);

        try {
            //send訊息
            for (int i = 1; i <= 10; i++) {
                kafkaProducer.send(new ProducerRecord<>("first", 2, "", "hello " + i), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            System.out.println("Topic : " + recordMetadata.topic() + ", Partition : " + recordMetadata.partition());
                        }
                    }
                });
            }
        } finally {
            //關閉
            kafkaProducer.close();
        }

    }

    @Test
    public void producerCallBackCustomPartition() {
        //設定
        Properties properties = new Properties();
        //連接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "K4:9092");
        //設定序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //設定自定義partitioner
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());

        //產生一個producer實體
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);

        try {
            //send訊息
            for (int i = 1; i <= 10; i++) {
                kafkaProducer.send(new ProducerRecord<>("first", "hi " + i), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            System.out.println("Topic : " + recordMetadata.topic() + ", Partition : " + recordMetadata.partition());
                        }
                    }
                });
            }
        } finally {
            //關閉
            kafkaProducer.close();
        }

    }


    @Test
    public void producerParameter() {
        //設定
        Properties properties = new Properties();
        //連接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "K4:9092");
        //設定序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //緩衝區大小(默認32M)
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64 * 1024 * 1024);
        //批次大小(16k)
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024);
        //liner ms(0ms)
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        //壓縮(snappy)
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        //產生一個producer實體
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);

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
        } finally {
            //關閉
            kafkaProducer.close();
        }
    }

    @Test
    public void producerAck() {
        //設定
        Properties properties = new Properties();
        //連接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "K4:9092");
        //設定序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //ack(dafault = all == -1)
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        //重試次數( Integer.MAX_VALUE)
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        //產生一個producer實體
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);

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
        } finally {
            //關閉
            kafkaProducer.close();
        }
    }
}
