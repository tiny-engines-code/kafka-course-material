package com.chrislomeli.kafka.kafkaspring;

import com.chrislomeli.kafka.kafkaspring.producer.GenericProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Used the Spring initializer with Lobok, Spring-kafka, and Spring configuration
 * Spring wraps some of the concepts we've already worked with in template classes
 * There are many ways to configure this kind of application, and to develop concurrency in Sping, so this example just gets the producer up and running
 * as a starter
 */
@SpringBootApplication
public class KafkaSpringApplication implements ApplicationRunner {

    @Value(value = "${spring.kafka.properties.schema.registry.url}")
    private String registry_url;

    @Value(value = "${topic.name}")
    private String topic;

    final GenericProducer genericProducer;

    public KafkaSpringApplication(GenericProducer genericProducer) {
        this.genericProducer = genericProducer;
    }

    /*
     worker function - just print some records as an example
     */
    public void examples(int recordCount) {
         genericProducer.sendMessages(recordCount);
    }

    /*
    * Same boiler-plate starter, but most properties now come from application.yaml
    * */
    public static void main(String[] args) {
        SpringApplication.run(KafkaSpringApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        String profile = "local.avro.properties";
        int recordCount = 10;
        if (args.getOptionNames().contains("records")) {
            recordCount = Integer.parseInt(args.getOptionValues("records").get(0));
        }

        examples(recordCount);

        System.exit(0);
    }
}
