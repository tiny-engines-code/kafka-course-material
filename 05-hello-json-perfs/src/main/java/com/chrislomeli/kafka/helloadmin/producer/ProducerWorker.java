package com.chrislomeli.kafka.helloadmin.producer;

import com.chrislomeli.kafka.helloadmin.generator.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class ProducerWorker {
    private final String topicName;
    private final KafkaProducer<String, User> producer;

    public ProducerWorker(String topicName, KafkaProducer<String, User> producer) {
        this.topicName = topicName;
        this.producer = producer;
    }

    public void onNext(User ns, Callback callback) {
        producer.send(new ProducerRecord<>(topicName, null, ns), callback);
    }

}
