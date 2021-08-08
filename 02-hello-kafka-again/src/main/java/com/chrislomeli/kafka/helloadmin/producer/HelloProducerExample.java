package com.chrislomeli.kafka.helloadmin.producer;

import com.chrislomeli.kafka.helloadmin.config.MyConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.util.StringUtils;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.lang.Thread.sleep;

@Slf4j
public class HelloProducerExample {

    public static void doExample(String topic, int records) {
        String bootStrapServers = StringUtils.collectionToCommaDelimitedString(MyConfiguration.servers);
        Properties clientProperties = new Properties();
        clientProperties.putAll(MyConfiguration.globalProperties);
        clientProperties.put(ProducerConfig.CLIENT_ID_CONFIG, MyConfiguration.applicationId);
        clientProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        clientProperties.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 10000);
        clientProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        clientProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        clientProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(clientProperties);

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

}
