package com.chrislomeli.kafka.helloadmin.producer;

import com.chrislomeli.kafka.helloadmin.generator.User;
import com.chrislomeli.kafka.helloadmin.generator.UserFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;

@Slf4j
public class ProducerJobRunner {

    KafkaProducer<String, User> producer;

    public void publishStringData(String topic, int records) {
        // Use a single Kafka producer for many threads
        Properties config = ProducerFactory.getProducerProperties();
        producer = new KafkaProducer<>(config);

        // data generator
        UserFactory generator = new UserFactory();

        log.info("\n------------------\nProduce {} records Async\n-----------------", records);
        Instant start = Instant.now();
        runAll(topic, records);
        long millis = Duration.between(start, Instant.now()).toMillis();
        log.info("\n-----Job Complete: Completed {} records in milliseconds={}----------", records, millis);

    }

    public void runAll(String topicName, int records) {
        log.info("\n------------------\nProduce {} records Async\n-----------------", records);
        // data generator
        UserFactory generator = new UserFactory();

        AtomicInteger sentCount = new AtomicInteger(0);

        for (int i = 0; i < records; i++) {
            User user = (User) generator.nextUser();
            producer.send(new ProducerRecord<>(topicName, user), (meta, e) -> {
                sentCount.incrementAndGet();
                if (e == null) {
                    log.debug("Callback:  Delivered [{}] at partition {}, offset {}", user.getUserName(), meta.partition(), meta.offset());
                } else {
                    log.info("Callback: FAILED to deliver [{}] : {}", user.getUserName(), e.getMessage());
                }
            });
        }
        log.info("Producer completed");

        while (sentCount.get() < records - 1) {
            log.info("waiting for completion");
            try {
                sleep(1000L);
            } catch (Exception ignored) {
            }
        }
        ;
    }


}
