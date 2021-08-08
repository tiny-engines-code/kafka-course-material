package com.chrislomeli.kafka.helloadmin.producer;

import com.chrislomeli.kafka.helloadmin.concurrent.FlowableController;
import com.chrislomeli.kafka.helloadmin.generator.IDatumFactory;
import com.chrislomeli.kafka.helloadmin.generator.NotificationStatusFactory;
import com.chrislomeli.kafka.helloadmin.producer.ProducerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Slf4j
public class ProducerJobRunner<K, V> {

    public void publishAvroData(String topic, int records) {
        // Use a single Kafka producer for many threads
        Properties config = ProducerFactory.getProducerProperties();
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(config);

        // data generator
        IDatumFactory<GenericRecord> generator = new NotificationStatusFactory<>();

        // create one (or more) publishers using this producer, generator - each publishing records
        log.info("create worker");
        FlowableController worker = new FlowableController(topic, producer, generator, records);

        log.info("\n------------------\nProduce {} records Async\n-----------------", records);
        Instant start = Instant.now();

        log.info("Starting the controller");
        worker.start();

        long millis = Duration.between(start, Instant.now()).toMillis();
        log.info("\n-----Job Complete: Completed {} records in milliseconds={}----------", records, millis);

        // metrics
        log.info("\n------------------\nProducer Metrics Available\n-----------------");
        getMetrics(producer);
    }

    public void getMetrics(KafkaProducer<String, GenericRecord> producer) {
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
