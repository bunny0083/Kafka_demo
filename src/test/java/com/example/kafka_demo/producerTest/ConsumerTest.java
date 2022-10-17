package com.example.kafka_demo.producerTest;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * @author Connor
 * @Date 2022-10-17 週一  14:37:21
 */
@SpringBootTest
public class ConsumerTest {

   @Test
   void consumer(){
      Properties properties = new Properties();

      properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "K3:9092,K5:9092");
      properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

      //設定groupid
      properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

      KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

      List<String> topics = List.of("first");
      kafkaConsumer.subscribe(topics);

      while (true){
         ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
         for (ConsumerRecord record : records){
            System.out.println(record);
         }
      }

   }
}
