package com.chrislomeli.kafka.kafkaspring;

import com.chrislomeli.kafka.kafkaspring.generator.NotificationStatus;
import com.chrislomeli.kafka.kafkaspring.generator.ServiceStatus;
import com.chrislomeli.kafka.kafkaspring.producer.GenericProducer;
import com.chrislomeli.kafka.kafkaspring.registry.GenericRecordFactory;
import com.chrislomeli.kafka.kafkaspring.registry.RegistryExample;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.print.attribute.standard.ReferenceUriSchemesSupported;
import java.util.Optional;

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

    @Value(value = "${notification_status_topic.name}")
    private String notificationStatusTopic;

    @Value(value = "${service_status_topic.name}")
    private String serviceStatusTopic;


    final GenericProducer genericProducer;

    public KafkaSpringApplication(GenericProducer genericProducer) {
        this.genericProducer = genericProducer;
    }

    /*
     worker function - just print some records as an example
     */
    public void examples(int recordCount) {

        Optional<String> notificationSchemaOpt = RegistryExample.doExample(notificationStatusTopic, registry_url, NotificationStatus.class, "notification_status.avsc");
        if (notificationSchemaOpt.isEmpty())
            throw new RuntimeException("Cannot find the topic for notification_status");
        Optional<String> serviceSchemaOpt = RegistryExample.doExample(serviceStatusTopic, registry_url, ServiceStatus.class, "service_status.avsc");
        if (serviceSchemaOpt.isEmpty())
            throw new RuntimeException("Cannot find the topic for service_status");
        // set schemas
        GenericRecordFactory.setSchema(notificationStatusTopic, NotificationStatus.class, notificationSchemaOpt.get());
        GenericRecordFactory.setSchema(serviceStatusTopic, ServiceStatus.class, serviceSchemaOpt.get());
        // create messages
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
        int recordCount = 10;
        if (args.getOptionNames().contains("notifications")) {
            recordCount = Integer.parseInt(args.getOptionValues("notifications").get(0));
        }

        examples(recordCount);

        System.exit(0);
    }
}
