package com.chrislomeli.springsandbox.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;


@Service
@Slf4j
public class ProducerService {

    final private KafkaTemplate<String, GenericRecord> kafkaTemplate;

    public ProducerService(KafkaTemplate<String, GenericRecord> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendProducerRecord(String topic, GenericRecord data)  {

        Accumulator.sent.incrementAndGet();
        ListenableFuture<SendResult<String, GenericRecord>> future = this.kafkaTemplate.send( topic, data );
        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, GenericRecord> result) {
                RecordMetadata meta = result.getRecordMetadata();
                Accumulator.success.incrementAndGet();
                log.debug("Delivered {}<--[{}] at partition={}, offset={}", meta.topic(), data.toString(), meta.partition(), meta.offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                Accumulator.failed.incrementAndGet();
                log.error(ex.getLocalizedMessage());
            }
        });
    }

    // todo - not using this right now
    public void getMetrics() {
        Set<String> WHITELIST = new HashSet<>(); // todo - there's a better way to filter
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

        final Map<MetricName, ? extends Metric> metrics = this.kafkaTemplate.metrics();
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