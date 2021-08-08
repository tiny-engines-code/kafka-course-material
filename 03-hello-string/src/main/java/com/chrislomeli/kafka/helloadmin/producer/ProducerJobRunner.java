package com.chrislomeli.kafka.helloadmin.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ProducerJobRunner {

    public void publishStringData(String topic, int records) {
        // Use a single Kafka producer for many threads
        Properties config = ProducerFactory.getProducerProperties();
        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

        log.info("\n------------------\nProduce {} records Blocking\n-----------------", records);
        for (int i = 0; i < records; i++) {
            try {
                String stringToSend = String.format("{\"userName\" : \"user%d\"}", i);
                RecordMetadata meta = producer.send(new ProducerRecord<>(topic, stringToSend)).get();
                log.debug("Callback:  Delivered [{}] at partition {}, offset {}", stringToSend, meta.partition(), meta.offset());

            } catch (InterruptedException | ExecutionException e) {
                log.error("Failed to produce!", e);
            }
        }
        log.info("Producer completed");

    }


}
