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
 * (1) This example uses executor threads to send concurrently to Kafka
 * It's generally accepted that there should be only one producer that is shared by all threads.
 * <p>
 * (2) We are dumping the producer metrics in this example
 * {@link com.chrislomeli.kafka.helloadmin.producer.ProducerJobRunner#getMetrics()}
 * *   - there are a lot of metrics, and this is only an example subset
 * - you can open this up by removing the if statement with the WHITELIST
 * we are filtering by a single group_name and a WHITELIST of meters
 * <p>
 * (3) as before we are calling the producer asynchronously but calling a stubbed dead-letter function
 * in the case of errors.  We would want to persist the messages in a different solution (AWS Firehose?)
 * and report and sweep the errors
 *
 * @Usage run> mvn clean package -Dmaven.test.skip=true
 * <p>
 * you can run the program using java or mvn:
 * run> java -jar --properties=<propertyfile/>  --records=<how-many-records-to-create/>
 * @Examples # send 500 records to my nsp topic
 * java -jar --properties=nsp.json.properties  --records=500  --threads=5
 * <p>
 * # send 500 records to my local confluent installation
 * java -jar --properties=local.json.properties  --records=500  --threads=5
 */

@Slf4j
@SpringBootApplication
public class KafkaApplication implements ApplicationRunner {

    public void examples(String profile, int recordCount, int threads) {

        MyConfiguration.initializeConfiguration(profile);

        String topic = MyConfiguration.topic;

        if (MyConfiguration.admin_client) AdminExample.doExample(topic);

        HelloProducerExample.doExample(topic, recordCount, threads);

        ConsumerExample.doExample(topic, "my-user-group");
    }

    public static void main(String... args) throws Exception {
        SpringApplication.run(KafkaApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        String profile = "local.json.properties";
        int recordCount = 10;
        int threads = 5;
        if (args.getOptionNames().contains("properties")) {
            profile = args.getOptionValues("properties").get(0);
        }
        if (args.getOptionNames().contains("records")) {
            recordCount = Integer.parseInt(args.getOptionValues("records").get(0));
        }
        if (args.getOptionNames().contains("threads")) {
            threads = Integer.parseInt(args.getOptionValues("threads").get(0));
        }

        examples(profile, recordCount, threads);

        System.exit(0);
    }
}
