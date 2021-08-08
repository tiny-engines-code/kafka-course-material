package com.chrislomeli.kafka.helloadmin;

import com.chrislomeli.kafka.helloadmin.config.MyConfiguration;
import com.chrislomeli.kafka.helloadmin.consumer.ConsumerExample;
import com.chrislomeli.kafka.helloadmin.producer.HelloProducerExample;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * In this example we not going to do much differently than the first
 * but we are breaking things like producer and consumer code into their own packages
 *
 * @Usage run> mvn clean package -Dmaven.test.skip=true
 * <p>
 * you can run the program using java or mvn:
 * run> java -jar --properties=<propertyfile/>  --records=<how-many-records-to-create/>
 * @Examples # send 500 records to my nsp topic
 * java -jar --properties=nsp.string.properties  --records=500
 * <p>
 * # send 500 records to my local confluent installation
 * java -jar --properties=local.string.properties  --records=500
 */
@Slf4j
@SpringBootApplication
public class KafkaApplication implements ApplicationRunner {

    public void examples(String profile, int recordCount) {

        MyConfiguration.initializeConfiguration(profile);

        String topic = MyConfiguration.topic;

        // break the producer specific code into a separate package
        HelloProducerExample.doExample(topic, recordCount);

        // break the consumer specific code into a separate package
        ConsumerExample.doExample(topic, "my-user-group");
    }

    /**
     * Boilerplate startup and configuration
     *
     */

    public static void main(String... args) throws Exception {
        SpringApplication.run(KafkaApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        String profile = "local.string.properties";
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
