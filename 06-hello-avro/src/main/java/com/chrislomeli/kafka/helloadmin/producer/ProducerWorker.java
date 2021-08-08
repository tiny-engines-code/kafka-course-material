package com.chrislomeli.kafka.helloadmin.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class ProducerWorker<K, V> {
    private String topicName;
    private KafkaProducer<K, V> producer;

    public ProducerWorker(String topicName, KafkaProducer<K, V> producer) {
        this.topicName = topicName;
        this.producer = producer;
    }

    public void onNext(V ns, Callback callback) {
        producer.send(new ProducerRecord<>(topicName, null, ns), callback);
    }

}
