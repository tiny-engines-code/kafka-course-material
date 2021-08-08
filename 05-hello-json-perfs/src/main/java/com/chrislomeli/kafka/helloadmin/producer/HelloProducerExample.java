package com.chrislomeli.kafka.helloadmin.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

import static java.lang.Thread.sleep;

@Slf4j
public class HelloProducerExample {

    public static void doExample(String topic, int records, int threads) {

        try {
            ProducerJobRunner example = new ProducerJobRunner();
            example.publishStringData(topic, records, threads);

        } catch (Exception e) {
            log.error("Failed to run producer", e);
        }
    }

}
