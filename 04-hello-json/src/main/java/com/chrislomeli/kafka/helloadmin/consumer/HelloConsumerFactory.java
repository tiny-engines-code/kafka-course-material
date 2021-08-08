package com.chrislomeli.kafka.helloadmin.consumer;

import com.chrislomeli.kafka.helloadmin.config.MyConfiguration;
import com.chrislomeli.kafka.helloadmin.generator.User;
import com.chrislomeli.kafka.helloadmin.serde.MyJsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.util.StringUtils;

import java.util.Properties;

public class HelloConsumerFactory {
    public static Properties getConsumerProperties(String groupId) {
        int pollRecordCount = 100;
        String location = "latest";

        String bootStrapServers = StringUtils.collectionToCommaDelimitedString(MyConfiguration.servers);
        // kafka producer setup
        Properties clientProperties = new Properties();
        clientProperties.putAll(MyConfiguration.globalProperties);
        clientProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "my-kafka-producer");
        clientProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        clientProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        clientProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        clientProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyConfiguration.value_deserializer);
        clientProperties.put(MyJsonDeserializer.VALUE_CLASS_NAME_CONFIG, User.class);

        // perf
        clientProperties.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 10000);
        clientProperties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
        clientProperties.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);

        // Consumption
        clientProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, pollRecordCount);
        clientProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, location);
        return clientProperties;
    }
}
