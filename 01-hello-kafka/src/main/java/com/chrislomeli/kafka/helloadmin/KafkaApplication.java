package com.chrislomeli.kafka.helloadmin;

import com.chrislomeli.kafka.helloadmin.config.MyConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Modified from the NSP example at:
 *
 * @link https://github.nike.com/NSP/examples/blob/master/producer_consumer/java/src/main/java/com/nike/nsp/examples/producerconsumer/Main.java
 * <p>
 * A simple hello world example that creates a producer, writes some strings to it, then creates a consumer to read the strings back.
 * <p>
 * This version moves the configurations into properties files.
 * These examples are not using Spring @Autowire because we want to see what's happening before we think about abstracting to Spring.
 * @Usage The way to run these examples is to:
 * (1) Choose a properties file - there are local.*.properties and nsp.*.properties versions
 * (2) Choose the log-level on the VM arguments
 * <p>
 * So, once you build the package
 * run> mvn clean package -Dmaven.test.skip=true
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
        String topic = MyConfiguration.topic;  // get the topic from properties.  this is because NSP topics are pre-set - you can't create them

        // do the work
        runProducer(topic, recordCount);
        runConsumer(topic, "my-user-group"); // hard code the consumer group for this example
    }

    /**
     * A Kafka producer takes a set of properties - there are many more properties, [See the KafkaProducer documentation]
     * But these are the ones you will typically see in a hello-world application
     * Most of these properties are hard-coded, but as we move forward, more will be in the properties files
     * The properties files are read by the MyConfiguration class
     * <p>
     * The KafkaProducer sends data to Kafka TOPICS
     * Each TOPIC is created with a set number of PARTITIONS - think of partitions as parallel "channels" that you can send data on fr a given TOPIC
     * So if we have 3 PARTITIONS, then we can send data on three concurrent listeners at once
     * All of the examples here will use the default round-robin allocation to the partitions - so you won't have to manage them - just send data and forget
     * Each PARTITION creates it's own file in the Kafka BROKER
     * <p>
     * Data is sent to a batch queue for the TOPIC.
     * Those queued records are batched and delivered to the BROKER when the queue is "full" or after a time lapse
     * <p>
     * There are three kinds of data that we can send:
     * KEY:  This is an group identifier.  All records with the same key will be processed on the same partition
     * An example of a key is a store identifier - where we want all of the data for one store to remain together
     * HEADERS:
     * VALUE:  The value is the actual data that is sent to Kafka (KEY and HEADERS could be considered meta-data)
     */
    public static void runProducer(String topic, int records) {
        /* create config properties */
        String bootStrapServers = StringUtils.collectionToCommaDelimitedString(MyConfiguration.servers);
        Properties clientProperties = new Properties();
        clientProperties.putAll(MyConfiguration.globalProperties);
        clientProperties.put(ProducerConfig.CLIENT_ID_CONFIG, MyConfiguration.applicationId);
        clientProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        clientProperties.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 10000);
        clientProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");  // snappy is the "best"
        /* Serializers
         *   Before the KafkaProducer sends data, it transforms it to bytes.
         *     When we later consume the data from the Kafka TOPIC we need to know how to re-assemble it
         *     Serializers pack the data into a format that can be re-assembled a certain way
         *     Deserializers know how to re-assemble the data
         *     StringSerializers and Deserializers just convert the flat String to bytes, but we will use other serialization methods (e.g. Json, Avro) later
         * */
        clientProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);  // the KEY get's it's own serializer - we will always use String
        clientProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class); // The VALUE gets it's own serializer as well - we will be changing this serializer
        /* injectors - we won't use any in this example, but before processing a producer to call whatever injectors we have configured
              it takes the output of the injector and makes that the new value
         */


        /*  Create the producer  */
        KafkaProducer<String, String> producer = new KafkaProducer<>(clientProperties);

        /* Send some records synchronously and wait for a response from the Broker
         *   The KafkaProducer returns a Future<RecordMetadata> - in this example we use .get() to wait for the data
         * */
        log.info("\n------------------\nProduce {} records Blocking\n-----------------", records);
        for (int i = 0; i < records; i++) {
            try {
                String stringToSend = String.format("{\"userName\" : \"user%d\"}", i);
                RecordMetadata meta = producer.send(new ProducerRecord<>(topic, stringToSend)).get();
                log.debug("Callback:  Delivered [{}] at partition {}, offset {}", stringToSend, meta.partition(), meta.offset());

            } catch (InterruptedException | ExecutionException e) {
                log.error("Failed to produce!", e);
            }
        }
        log.info("Producer completed");
    }

    /**
     * A Kafka consumer takes a set of properties - there are many more properties, [See the KafkaConsumer documentation]
     * But these are the ones you will typically see in a hello-world application
     * Most of these properties are hard-coded, but as we move forward, more will be in the properties files
     * The properties files are read by the MyConfiguration class
     * <p>
     * The KafkaConsumer polls for a preset amount of data (MAX_POLL_RECORDS_CONFIG) at a time
     * A KafkaConsumer::pol() function is usually in an infinite loop that gets records as they become available
     * Because these examples are creating a set amount of records, we set the poll(Duration) to 5 seconds
     * The result of that is that
     * (1) the program will "hang" for 5 seconds at the end - then it will exit because it has timed-out
     * (2) on really slow systems the consumer might exit without getting all of the latest records - they will get picked up the next time we consume
     * <p>
     * How does that consumer know where you last left-off?
     * The GROUP_ID_CONFIG is a named string that keeps track of the OFFSETS in each PARTITION for a give TOPIC
     * example:
     * User 1 is reading from a topic called "notification_status" using a consumer group called "NGAP"
     * User 2 is also using "NGAP"
     * User 3, 4 and 5 are all using consumer group "CDS"
     * <p>
     * User 1 starts reading topic "notification_status" using the "NGAP" consumer group.
     * - They read records 0 thru 2000, then stop reading.
     * User 2 then starts reading using the consumer group called "NGAP"
     * - they will get 2000 thru the end of the topic
     * <p>
     * User 3 starts reading topic "notification_status" using a consumer group called "CDS"
     * Because they are on the "CDS" consumer group they will get records 0 thru the end of the topic
     */
    public static void runConsumer(String topic, String consumerGroup) {

        /* create config properties */
        String bootStrapServers = StringUtils.collectionToCommaDelimitedString(MyConfiguration.servers);

        Properties clientProperties = new Properties();
        clientProperties.putAll(MyConfiguration.globalProperties);
        clientProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-kafka-producer");
        clientProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        clientProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        clientProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        clientProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        /* Deserializers know how to re-assemble the data - in this case it's just a simple conversion from bytes to String
         * */
        clientProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        clientProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        /* create a KafkaConsumer */
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(clientProperties);

        /* A Kafka consumer polls for records with a pre-set timeout
         *   and uses the Deserializer to read bytes back into a readable format
         */
        log.info("\n------------------\nConsume records from earliest checkpoint in consumer group: {}\n-----------------", consumerGroup);
        // a consumer "subscribes" to a topic (actually a list of topics)
        consumer.subscribe(Collections.singletonList(topic));

        /* (a consumer can also read a specific partition, and can also 'seek' to any location in the partition) */

        long count = 0;
        while (true) {
            // poll for 5 seconds then, drop out
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            // if we have not received anything in 5 seconds then exit the infinite loop - this is just for training
            if (records.isEmpty())
                break;

            // read whatever records we received
            for (ConsumerRecord<String, String> consumedRecord : records) {
                log.debug("Consumer received={} at partition={}, offset={}", consumedRecord.value(), consumedRecord.partition(), consumedRecord.offset());
                count++;
            }
        }
        consumer.close();
        log.info("Consumer read back last {} records ", count);
    }


    /**
     * For all of the examples we are going to use a spring-initializer project just to keep everything conformed
     * But we are not going to use many spring-specific context values.  We will not use @Autowire
     * Instead we create a MyConfiguration class that does much the same thing as the @Value annotation - it reads properties into variables
     * And instead injection we'll either pass parameters or use the static MyConfiguration variables
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
