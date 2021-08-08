package com.chrislomeli.kafka.helloadmin.concurrent;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

@Slf4j
class FlowableWorker {

    private final String topicName;
    private final KafkaProducer<String, GenericRecord> producer;

    public FlowableWorker(String topic, KafkaProducer<String, GenericRecord> producer) {
        this.topicName = topic;
        this.producer = producer;
    }

    public void onNext(GenericRecord datum) {
        Accumulator.delivered.incrementAndGet();
        log.debug("Entering the worker...");
        producer.send(new ProducerRecord<>(topicName, datum), (metadata, e) -> {
            Accumulator.responses.incrementAndGet();
            if (e != null) {
                deadLetters(datum, e);
            } else {
                success(datum, metadata);
            }
        });
        log.debug("Leaving the worker...");
    }

    public void deadLetters(GenericRecord record, Exception e) {
        log.error("Send {} to deadletter queue", record);
    }

    public void success(GenericRecord datum, RecordMetadata metadata) {
        log.debug("Callback:  Delivered [{}] at partition {}, offset {}", datum.toString(), metadata.partition(), metadata.offset());
    }
}

