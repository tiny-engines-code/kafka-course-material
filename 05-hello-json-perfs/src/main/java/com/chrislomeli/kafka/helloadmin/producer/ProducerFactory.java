package com.chrislomeli.kafka.helloadmin.producer;

import com.chrislomeli.kafka.helloadmin.config.MyConfiguration;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.util.StringUtils;

import java.util.Properties;

public class ProducerFactory {

    /*
    Kafka uses 2 libraries for its metrics:
    "Yammer" metrics: These are used mostly on the broker side

    Kafka metrics: Kafka created its own metrics library and these are used in the clients.

    As you may know, there's a bunch of common code (network, requests) that is used by both the broker and client side. As this code lives in the client side project this results in the broker having both types of metrics hence the 2 reporter types!

    kafka.metrics.reporter is for the "Yammer metrics"
    metric.reporters is for the "Kafka metrics"
    Which one to use depends on what you want to see. You can have a custom reporter implement both interfaces if you want all the metrics. Also as all the metrics can also be made available via JMX, you may want to scrap that instead of relying on metrics reporters. Both solutions work in practice.
     */
    public static Properties getProducerProperties() {

        String bootStrapServers = StringUtils.collectionToCommaDelimitedString(MyConfiguration.servers);
        // kafka producer setup
        Properties clientProperties = new Properties();
        clientProperties.putAll(MyConfiguration.globalProperties);
        clientProperties.put(ProducerConfig.CLIENT_ID_CONFIG, MyConfiguration.applicationId);
        clientProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        clientProperties.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 10000);
        clientProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        clientProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        clientProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MyConfiguration.value_serializer);
        clientProperties.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);

        //--- todo : clean these up and make them configurable
        clientProperties.put(ProducerConfig.RETRIES_CONFIG, 3);
        clientProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
        clientProperties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        clientProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        clientProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384 * 4); // Batch up to 64K buffer in the queue.
        clientProperties.put(ProducerConfig.LINGER_MS_CONFIG, 100); // wait this long before de-queue regardless of queue size
        return clientProperties;
    }


}
