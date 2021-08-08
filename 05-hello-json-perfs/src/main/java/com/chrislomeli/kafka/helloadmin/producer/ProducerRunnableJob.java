package com.chrislomeli.kafka.helloadmin.producer;

import com.chrislomeli.kafka.helloadmin.generator.User;
import com.chrislomeli.kafka.helloadmin.generator.UserFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.sleep;

@Slf4j
public class ProducerRunnableJob implements Runnable {

    private Thread worker;
    int records;
    ProducerWorker producerWorker;
    UserFactory userFactory;
    AtomicInteger sentCount = new AtomicInteger(0);

    public ProducerRunnableJob(String topicName, KafkaProducer producer, UserFactory generator, int records) {
        Properties config = ProducerFactory.getProducerProperties();
        producerWorker = new ProducerWorker(topicName, producer);
        this.records = records;
        this.userFactory = generator;
    }

    public void start() {
        worker = new Thread(this);
        worker.start();
    }


    @Override
    public void run() {
        log.info("Produce {} records", records);
        for (int i = 0; i < records; i++) {
            sentCount.incrementAndGet();
            User nextValue = userFactory.nextUser();

            // call worker with callback
            producerWorker.onNext(nextValue, (metadata, e) -> {
                if (e != null) {
                    log.error("failed to send", e);
                    deadLetters(nextValue);
                } else {
                    log.debug("Callback:  Delivered [{}] at partition {}, offset {}", nextValue, metadata.partition(), metadata.offset());
                }
            });
        }
        log.info("Producer completed");

        // wait for all to finish just to see it working
        log.info("Waiting for {} records", records);
        while (sentCount.get() < records) {
            log.info("waiting for completion");
            try {
                sleep(1000L);
            } catch (Exception ignored) {
            }
        }

        worker.interrupt();
    }

    public void deadLetters(User record) {
        log.error("Send {} to deadletter queue", record);
    }
}
