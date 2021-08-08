package com.chrislomeli.kafka.helloadmin;

import com.chrislomeli.kafka.helloadmin.admin.AdminExample;
import com.chrislomeli.kafka.helloadmin.config.MyConfiguration;
import com.chrislomeli.kafka.helloadmin.consumer.ConsumerExample;
import com.chrislomeli.kafka.helloadmin.producer.HelloProducerExample;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * In this example we are going to experiment with serializers.
 * Check out the lines in the local.json.properties file
 * - value_deserializer=com.chrislomeli.kafka.helloadmin.serde.MyJsonDeserializer
 * - value_serializer=com.chrislomeli.kafka.helloadmin.serde.MyJsonSerializer
 * <p>
 * This example contains some new features:
 * <p>
 * producer changes:
 * (1) Instead of calling the Producer synchronously we are using an anonymous callback function to update the status
 * Because we are exiting the program we need to 'wait' for all of the callbacks to come back before exiting
 * While we might not do this in a long running process, we still need a way to know that our futures are backing up
 * We can do this by tracking responses, or by dumping the metrics that we'll see in the next example every so often
 * (2) Instead of the String serializers and deserializers we are now using JSON serializers and deserializers
 * Check out the lines in the local.json.properties file
 * - value_deserializer=com.chrislomeli.kafka.helloadmin.serde.MyJsonDeserializer
 * - value_serializer=com.chrislomeli.kafka.helloadmin.serde.MyJsonSerializer
 * <p>
 * serde package:
 * We are going to send POJO's to the producer but our serializer will use "Jackson" marshal our Pojo to string bytes for Kafka
 * and our deserializer will unmarshal them back to POJO
 * <p>
 * generator package:
 * This package contains an example User POJO and a factory class to generate "random" users
 *
 * Usage run> mvn clean package -Dmaven.test.skip=true
 * <p>
 * you can run the program using java or mvn:
 * run> java -jar --properties=<propertyfile/>  --records=<how-many-records-to-create/>
 * Examples # send 500 records to my nsp topic
 * java -jar --properties=nsp.json.properties  --records=500
 * <p>
 * # send 500 records to my local confluent installation
 * java -jar --properties=local.json.properties  --records=500
 */

@Slf4j
@SpringBootApplication
public class KafkaApplication implements ApplicationRunner {

    public void examples(String profile, int recordCount) {

        MyConfiguration.initializeConfiguration(profile);

        String topic = MyConfiguration.topic;

        if (MyConfiguration.admin_client) AdminExample.doExample(topic);

        HelloProducerExample.doExample(topic, recordCount);

        ConsumerExample.doExample(topic, "my-user-group");
    }

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
