package com.chrislomeli.kafka.helloadmin.consumer;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Data
@Slf4j
public class ConsumerExample implements AutoCloseable {

    KafkaConsumer<String, String> consumer;
    String consumerGroup;

    // We are not focusing as much on the consumer, so we don't break out this static factory from the non-static instance
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
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty())
                break;
            for (ConsumerRecord<String, String> consumedRecord : records) {
                log.info("Consumer received={} at partition={}, offset={}", consumedRecord.value(), consumedRecord.partition(), consumedRecord.offset());
                count++;
            }
        }
        log.info("Consumer read back last {} records ", count);
    }


    @Override
    public void close() {
        consumer.close();
    }
}
