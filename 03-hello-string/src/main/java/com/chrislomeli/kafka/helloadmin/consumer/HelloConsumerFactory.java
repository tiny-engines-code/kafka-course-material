package com.chrislomeli.kafka.helloadmin.consumer;

import com.chrislomeli.kafka.helloadmin.config.MyConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.util.StringUtils;

import java.util.Properties;

public class HelloConsumerFactory {
    public static Properties getConsumerProperties(String groupId) {

        String bootStrapServers = StringUtils.collectionToCommaDelimitedString(MyConfiguration.servers);
        // kafka producer setup
        Properties clientProperties = new Properties();
        clientProperties.putAll(MyConfiguration.globalProperties);
        clientProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-kafka-producer");
        clientProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        clientProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        clientProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        clientProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        clientProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        clientProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return clientProperties;
    }
}
