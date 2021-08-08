package com.chrislomeli.kafka.helloadmin.consumer;

import com.chrislomeli.kafka.helloadmin.config.MyConfiguration;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Data
@Slf4j
public class ConsumerExample {

    KafkaConsumer<String, String> consumer;
    String consumerGroup;

    public static void doExample(String topic, String consumerGroup) {

        String bootStrapServers = StringUtils.collectionToCommaDelimitedString(MyConfiguration.servers);

        Properties clientProperties = new Properties();
        clientProperties.putAll(MyConfiguration.globalProperties);
        clientProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-kafka-producer");
        clientProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        clientProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        clientProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        clientProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        clientProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        clientProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(clientProperties);

        log.info("\n------------------\nConsume records from earliest checkpoint in consumer group: {}\n-----------------", consumerGroup);
        consumer.subscribe(Collections.singletonList(topic));
        long count = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty())
                break;
            for (ConsumerRecord<String, String> consumedRecord : records) {
                log.debug("Consumer received={} at partition={}, offset={}", consumedRecord.value(), consumedRecord.partition(), consumedRecord.offset());
                count++;
            }
        }
        consumer.close();
        log.info("Consumer read back last {} records ", count);
    }

}
