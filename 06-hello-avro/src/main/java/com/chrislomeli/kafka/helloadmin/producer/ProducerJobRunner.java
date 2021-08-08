package com.chrislomeli.kafka.helloadmin.producer;

import com.chrislomeli.kafka.helloadmin.generator.UserFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ProducerJobRunner<K, V> {

    KafkaProducer<K, V> producer;
    List<ProducerRunnableJob> publishers;

    public void publishAvroData(String topic, int recordsPerWorker, int threadCount) {
        // Use a single Kafka producer for many threads
        Properties config = ProducerFactory.getProducerProperties();
        producer = (KafkaProducer<K, V>) new KafkaProducer<String, GenericRecord>(config);

        // data generator
        UserFactory generator = new UserFactory<GenericRecord>();

        // create one (or more) publishers using this producer, generator - each publishing records
        publishers = new ArrayList<>();
        for (int i = 0; i < threadCount; i++)
            publishers.add(new ProducerRunnableJob(topic, producer, generator, recordsPerWorker));

        int totalSendCount = recordsPerWorker * publishers.size();

        log.info("\n------------------\nProduce {} records Async\n-----------------", totalSendCount);
        Instant start = Instant.now();
        runAll();
        long millis = Duration.between(start, Instant.now()).toMillis();
        log.info("\n-----Job Complete: Completed {} records in milliseconds={}----------", totalSendCount, millis);

        // metrics
        log.info("\n------------------\nProducer Metrics Available\n-----------------");
        getMetrics();
    }

    public void runAll() {
        ExecutorService executor = Executors.newFixedThreadPool(publishers.size() + 1);
        for (ProducerRunnableJob publisher : publishers) {
            executor.submit(publisher);
        }

        executor.shutdown();
        try {
            log.info("Awaiting termination");
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Failed in runAll()", e);
        }
    }


    public void getMetrics() {
        Set<String> WHITELIST = new HashSet<>();
        WHITELIST.add("compression-rate-avg");
        WHITELIST.add("response-rate");
        WHITELIST.add("request-rate");
        WHITELIST.add("request-latency-avg");
        WHITELIST.add("outgoing-byte-rate");
        WHITELIST.add("io-wait-time-ns-avg");
        WHITELIST.add("batch-size-avg");
        WHITELIST.add("record-send-total");
        WHITELIST.add("records-per-request-avg");
        WHITELIST.add("record-queue-time-avg");
        WHITELIST.add("record-queue-time-max");
        WHITELIST.add("record-error-rate");
        WHITELIST.add("record-retry-total");

        final Map<MetricName, ? extends Metric> metrics = producer.metrics();
        for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
            MetricName mn = entry.getValue().metricName();
            Object mv = entry.getValue().metricValue();
            if ("producer-metrics".equals(mn.group()) && WHITELIST.contains(mn.name())) {
                log.info("name={}, group={}, desc={}, mv={}", mn.name(), mn.group(),
                        mn.description(),
                        mv);

            }
        }
    }
}
