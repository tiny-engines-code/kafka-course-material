package com.chrislomeli.kafka.helloadmin;

import com.chrislomeli.kafka.helloadmin.admin.AdminExample;
import com.chrislomeli.kafka.helloadmin.config.MyConfiguration;
import com.chrislomeli.kafka.helloadmin.consumer.ConsumerExample;
import com.chrislomeli.kafka.helloadmin.generator.NotificationStatus;
import com.chrislomeli.kafka.helloadmin.producer.HelloProducerExample;
import com.chrislomeli.kafka.helloadmin.registry.GenericRecordFactory;
import com.chrislomeli.kafka.helloadmin.registry.RegistryExample;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Optional;

/**
 * This purely experimental version replaces executor threads with RxJava flows
 * In this setup the subscriber can throttle the controller, but we are not actually throttling
 * The method we use to throttle could depend on metrics or using the Accumulator to determine how many Futures<> we are still waiting on
 * This method should allow for multiple workers, but I did not get to that.
 * Also, the subscriber should have an OnError() function.
 */
@Slf4j
@SpringBootApplication
public class KafkaApplication implements ApplicationRunner {

    public void examples(String profile, int recordCount) {

        MyConfiguration.initializeConfiguration(profile);

        String topic = MyConfiguration.topic;

        if (MyConfiguration.admin_client)
            AdminExample.doExample(topic);

        if (MyConfiguration.use_schema) {
            Optional<String> schemaStringOption = RegistryExample.doExample(topic, NotificationStatus.class, "notification_status.avsc");
            schemaStringOption.ifPresent(s -> MyConfiguration.schema_string = s); // if we can get a schema from the remote registry then use that
            GenericRecordFactory.setSchema(MyConfiguration.schema_string);
        }

        HelloProducerExample.doExample(topic, recordCount);

        ConsumerExample.doExample(topic, "my-user-group");
    }

    public static void main(String... args) throws Exception {
        SpringApplication.run(KafkaApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        String profile = "local.avro.properties";
        int recordCount = 10;
        if (args.getOptionNames().contains("properties")) {
            profile = args.getOptionValues("properties").get(0);
        }
        if (args.getOptionNames().contains("records")) {
            recordCount = Integer.parseInt(args.getOptionValues("records").get(0));
        }

        examples(profile, recordCount);

        System.exit(0);
    }
}
