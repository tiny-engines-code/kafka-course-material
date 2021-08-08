package com.chrislomeli.kafkautil.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.boot.configurationprocessor.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class KafkaProducerService<T> implements AutoCloseable {

    Properties properties;
    KafkaProducer<String, T> producer;

    public KafkaProducerService(Properties properties) {
        log.info("Enter KafkaProducerService");
        this.properties = properties;
        producer = new KafkaProducer<String, T>(properties);
        log.info("instantiated KafkaProducerService...");
    }

    @Override
    public void close() {
        if (producer != null)
            producer.close();
    }

    public Map<String,Integer> sendStringRecords(String topicName, List<T> nl) {
        int sends = 0;
        int failed = 0;
        for (T ns : nl) {
            log.info("Sending to {}: {}", topicName, ns);
            try {
                RecordMetadata metadata = producer.send(new ProducerRecord<String,T>(topicName, ns))
                        .get();
                long offset = metadata.offset();
                log.info("Success for : {}", ns);
                sends++;
            } catch (InterruptedException | ExecutionException e) {
                failed++;
                log.error("Failed for : {}: {}", ns, e);
            }

        }
        Map<String, Integer> out = new HashMap<>();
        out.put("sent", sends);
        out.put("failed", failed);
        return out;
    }

    public void sendRecord(String topicName, T record) throws ExecutionException, InterruptedException {
        RecordMetadata metadata = producer.send(new ProducerRecord<>(topicName, record)).get();
        long offset = metadata.offset();
        log.info("Send Success for : {}", topicName);
    }
}