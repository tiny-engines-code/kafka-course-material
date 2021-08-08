package com.chrislomeli.kafka.helloadmin.consumer;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Data
@Slf4j
public class ConsumerExample implements AutoCloseable {

    KafkaConsumer<String, GenericRecord> consumer;
    String consumerGroup;

    public static void doExample(String topic, String consumerGroup) {
        try (ConsumerExample example = new ConsumerExample(consumerGroup)) {
            example.consumeTopic(topic);
        } catch (Exception e) {
            log.error("failed consumer", e);
        }
    }

    public ConsumerExample(String consumerGroup) {
        Properties config = HelloConsumerFactory.getConsumerProperties(consumerGroup);
        this.consumer = new KafkaConsumer<>(config);
        this.consumerGroup = consumerGroup;
    }

    public void consumeTopic(String topic) {
        log.info("\n------------------\nConsume records from earliest checkpoint in consumer group: {}\n-----------------", this.consumerGroup);
        consumer.subscribe(Collections.singletonList(topic));
        long count = 0;
        while (true) {
            ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty())
                break;
            for (ConsumerRecord<String, GenericRecord> consumedRecord : records) {
                log.info("Consumer received={} at partition={}, offset={}", consumedRecord.value(), consumedRecord.partition(), consumedRecord.offset());
                count++;
            }
        }
        log.info("Consumer read back last {} records ", count);
    }


    @Override
    public void close() throws Exception {
        consumer.close();
    }
}
