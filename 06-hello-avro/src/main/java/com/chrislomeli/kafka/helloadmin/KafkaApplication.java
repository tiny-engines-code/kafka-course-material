package com.chrislomeli.kafka.helloadmin;

import com.chrislomeli.kafka.helloadmin.admin.AdminExample;
import com.chrislomeli.kafka.helloadmin.config.MyConfiguration;
import com.chrislomeli.kafka.helloadmin.consumer.ConsumerExample;
import com.chrislomeli.kafka.helloadmin.generator.User;

import java.util.Optional;

import com.chrislomeli.kafka.helloadmin.producer.HelloProducerExample;
import com.chrislomeli.kafka.helloadmin.producer.ProducerJobRunner;
import com.chrislomeli.kafka.helloadmin.registry.GenericRecordFactory;
import com.chrislomeli.kafka.helloadmin.registry.RegistryExample;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * In this example we are sending data to Kafka in AVRO format
 * (1) we need to define a schema and make sure that other programs such as KafkaConnect can use the schema
 * (2) no longer using the JSON serializers, instead we use the Clonfluent Avro serde (serializer/deserializer)
 * <p>
 * Schema
 * - Avro uses a schema that it can use to read the data
 * - It's possible to embed the schema in evert payload we send to Kafka - but we don't want to do that
 * - It's possible to keep a copy of the schema in Git and use it in both the producer and the consumer - but other programs like KafkaConnect will not be able to read the topic
 * - The accepted way to handle this is to have a separate schema registry that attaches to the Kafka Broker
 * - There are a few schema registry implementations, but Nike uses the Confluent schema registry.
 * <p>
 * The Confluent Schema Registry
 * - Resides on a separate server
 * - Keeps a copy of the schema in a topic called "_Schema" in the Kafka broker
 * - Provides a Rest interface we can use to manage our schemas - usually listens on port 8081
 * - Be aware that versioning is tricky - if you don't have the complete ability to manage the schema you should be very careful about changing it.
 * - It's easy to get into a situation where you can't change something simple and can't figure out why (e.g. a change in the namespace name can get you stuck and if you can't compare the schemas to see why its' failing this can be frustrating)
 * <p>
 * Specific vs Generic data
 * Specific records
 * todo: provide an example of a specific class abd arvc file
 * - With specific data you can seem to send a Pojo directly to Kafka bu that "pojo" has a hard coded internal index map and a hard-copy of the schema
 * - so that pojo is not like any other in you model and if your hard-coded schema gets out of sync with the Registry you will have to troubleshoot
 * - with NSP's very limited access to Kafka, troubleshooting will ahve to involce the NSP team in some cases
 * <p>
 * GenericRecords
 * - A Generic record is an already indexed "map" of the data in your pojo - think of it as a HashMap with some internal indexing that the producer can use
 * - The idea is that it's easier just to do this up-front with reflection and you can store a real pojo in you models package
 * - Instead of keeping a hard-coded copy locally, just get th
 * <p>
 * - The Confluent Avro serializer then
 * For a SpecificRecord
 * - reads your local schema (if you provide a Specific Record)
 * - checks for the schema in the schema registry
 * - if the schema does not exist and you have set auto-register to true it creates the remote scema using your local schema
 * (don't do this is production)  todo: provide an auto-register configuration example
 * - compares the local schema to the remote schema (if you are using a SpecificRecord)
 * - Marshals your SpecifcRecord to a GenericRecord
 * <p>
 * For both SpecificRecord and GenericRecords
 * - Places a 'magic byte '0' at the start of the binary data
 * - gets the id number from the schema in the schema registry and places in in the next four bytes
 * - converts each indexed field into a binary chunk and adds it to the bytes data
 * - places the bytes data as a single record in the producer buffer (queue)
 * - when the producer buffer reaches max size, or a time interval has been hit - it sends a request to Kafka with the contents of the buffer
 * <p>
 * This example shows one way of handling Avro without using the SpecificRecord.  We:
 * To setup the example we:
 * - manually create a schema in {@link RegistryExample#doExample(String, Class)}
 * - and store it to the schema registry using the RestClient wrapper from Confluent (I originally just used Okhttp)
 * Then we process as we would in production:
 * - We retrieve the schema from production
 * - We validate the schema against the Pojo we are going to be using {@link  com.chrislomeli.kafka.helloadmin.registry.SchemaValidatorService#validateClass(Schema, Class)}
 * - The we set it on a GenericRecordFactory: {@link  com.chrislomeli.kafka.helloadmin.registry.GenericRecordFactory#setSchema(Schema)}
 * - before sending we use the GenericRecordFactory to transform our Pojo to a GenericRecord at {@link  com.chrislomeli.kafka.helloadmin.registry.GenericRecordFactory#fromPojo(Object)}
 * - then send the GenericRecord to the Producer
 * <p>
 * (2) We are also dumping the producer metrics in this example
 * - We get the metrics from the Producer at {@link ProducerJobRunner#getMetrics()}
 * *   - there are a lot of metrics, and this is only an example subset
 * - you can open this up by removing the if statement with the WHITELIST
 * we are filtering by a single group_name and a WHITELIST of meters
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

        if (MyConfiguration.use_schema) {
            Optional<String> schemaStringOption = RegistryExample.doExample(topic, User.class);
            schemaStringOption.ifPresent(s -> MyConfiguration.schema_string = s); // if we can get a schema from the remote registry then use that
            GenericRecordFactory.setSchema(MyConfiguration.schema_string);
        }

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
