package com.nike.nsp.examples.producerconsumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

// This file covers a minimal set of configuration including those that are specific to the NSP implementation of
// Kafka. More complete lists of available configuration for Kafka producers and consumers are given at:
//    Producers: https://docs.confluent.io/current/installation/configuration/producer-configs.html#cp-config-producer
//    Consumers: https://docs.confluent.io/current/installation/configuration/consumer-configs.html#cp-config-consumer

@Slf4j
public class MainTest {


    // TODO: User-supplied configuration. These values should be changed to match your use case.

    //    // The URL of the broker to connect to.
    //    final static String BROKER_URL = "<returned from gem after creating source and sink>";
    //
    //    // The name of the topic to connect to.
    //    final static String TOPIC = "<returned from gem after creating source and sink>";
    //
    //    // The "token" endpoint for the OAuth server.
    //    final static String OAUTH_TOKEN_URL = "<returned from gem after creating source and sink>";
    //
    //    // The client ID of the authorized user.
    //    final static String OAUTH_CLIENT_ID = "<client id from developer portal / user in gem source and sink>";
    //
    //    // The client secret of the authorized user.
    //    final static String OAUTH_CLIENT_SECRET = "<client secret from developer portal>";

    // TODO: If using a VPC Endpoint to connect to NSP then this setting will need to be applied
    //
    // The local location of the truststore JKS file. This can be downloaded from
    // https://github.nike.com/NSP/nsp-customer-resources/blob/master/certs/gcl.prd.nsp.nike.com.jks.
    final static String JKS_LOCATION = "https://github.nike.com/NSP/nsp-customer-resources/blob/master/certs/gcl.prd.nsp.nike.com.jks";

    // The client ID to use for the producer. This should uniquely identity this producer instance.
    final static String CLIENT_ID = "test";

    // The group ID to use for the consumer. This should uniquely identity this consumer instance.
    final static String GROUP_ID = "test";

    // The URL of the broker to connect to.
    static String BROKER_URL;

    // The name of the topic to connect to.
    static String TOPIC;

    // The "token" endpoint for the OAuth server.
    static String OAUTH_TOKEN_URL;

    // The client ID of the authorized user.
    static String OAUTH_CLIENT_ID;

    // The client secret of the authorized user.
    static String OAUTH_CLIENT_SECRET;

    static String JAAS_CONFIG;



    public static void main(String[] args) {

        // get from external file
        Properties prop = new Properties();
        String envFile = "nsp.properties";
        File file = new File(envFile);
        if (file.exists() && !file.isDirectory()) {

            try (InputStream input = new FileInputStream(envFile)) {
                prop.load(input);
            } catch (IOException ignored) {
            }

        }

        if (prop.size() == 0) {
            return;
        }

        // The URL of the broker to connect to.
        BROKER_URL = (String) prop.get("servers");

        // The name of the topic to connect to.
        TOPIC = (String) prop.get("topic");

        // The "token" endpoint for the OAuth server.
        OAUTH_TOKEN_URL = (String) prop.get("token_url");

        // The client ID of the authorized user.
        OAUTH_CLIENT_ID = (String) prop.get("client_id");

        // The client secret of the authorized user.
        OAUTH_CLIENT_SECRET = (String) prop.get("client_secret");

        JAAS_CONFIG = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " + String.format("oauth.token.endpoint.uri=\"%s\" ", OAUTH_TOKEN_URL) +
                String.format("oauth.client.id=\"%s\" ", OAUTH_CLIENT_ID) +
                String.format("oauth.client.secret=\"%s\" ;", OAUTH_CLIENT_SECRET);

        // Global configuration for getting JWTs
        // See https://github.com/strimzi/strimzi-kafka-oauth#configuring-the-kafka-client
//        System.setProperty("oauth.token.endpoint.uri", OAUTH_TOKEN_URL);
//        System.setProperty("oauth.client.id", OAUTH_CLIENT_ID);
//        System.setProperty("oauth.client.secret", OAUTH_CLIENT_SECRET);

        // Produce message
        Producer<String, String> producer = getProducer(BROKER_URL);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "{\"name\": \"cheesehead\"}");
        try {
            RecordMetadata res = producer.send(record).get();
            System.out.println("partition,offset = " + res.partition() + "," + res.offset());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        producer.close();

        // Consume messages
        Consumer<String, String> consumer = getConsumer(BROKER_URL, TOPIC);
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(60));
        for (ConsumerRecord<String, String> consumedRecord : records) {
            System.out.println("received = " + consumedRecord.value());
        }
        consumer.close();
    }

    private static Producer<String, String> getProducer(String bootstrapServers) {

        // Connection
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);

        // Auth
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, JAAS_CONFIG);
//        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        props.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");

        // Serialization / Deserialization
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // TODO: If using a VPC Endpoint to connect to NSP then these settings will need to be applied
//         props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
//         props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, JKS_LOCATION);

        // Return the producer
        return new KafkaProducer<>(props);
    }

    private static Consumer<String, String> getConsumer(String bootstrapServers, String topic) {

        // Connection
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        // Auth
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, JAAS_CONFIG);
//        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
        props.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");

        // Serialization / Deserialization
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // Consumption
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // TODO: If using a VPC Endpoint to connect to NSP then these settings will need to be applied
//         props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
//         props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, JKS_LOCATION);

        // Return the consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }
}