package com.chrislomeli.kafkaspring.producer;

import com.chrislomeli.kafka.kafkaspring.producer.GenericProducer;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@NoArgsConstructor
@Component
public class HelloProducerExample {

    GenericProducer genericProducer;

    @Autowired
    public HelloProducerExample(GenericProducer genericProducer) {
        this.genericProducer = genericProducer;
    }

    public void run(int records) {
        this.genericProducer.sendMessages(records);
    }

}
