package com.chrislomeli.kafka.helloadmin.producer;

import com.chrislomeli.kafka.helloadmin.generator.User;
import com.chrislomeli.kafka.helloadmin.producer.ProducerJobRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

import static java.lang.Thread.sleep;

@Slf4j
public class HelloProducerExample {

    public static void doExample(String topic, int records) {

        try {
            ProducerJobRunner example = new ProducerJobRunner();

            example.publishStringData(topic, records);

        } catch (Exception e) {
            log.error("Failed to run producer", e);
        }
    }

}
