package com.chrislomeli.kafkautil;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.*;

@Slf4j
@Data
public class ServiceConfiguration {
    // Singleton
    static ServiceConfiguration serviceConfiguration;
    static String envPropertiesFile = "local.properties";

    public List<String> servers;
    public String topic;
    public String token_url;
    public String client_id;
    public String client_secret;
    public String registry_url;
    public String schema_string;
    public Integer page_size;
    public Boolean security = false;
    public Boolean admin_client = false;
    public Boolean use_schema = false;
    public Class<?> value_serializer;
    public Class<?> value_deserializer;

    public static Properties globalProperties;

    public enum SERIALIZER {
        JSON,
        AVRO,
        STRING
    }

    public static ServiceConfiguration getInstance() {
        if (serviceConfiguration == null)
            serviceConfiguration = new ServiceConfiguration();
        return serviceConfiguration;
    }

    public ServiceConfiguration() {
        globalProperties = new Properties();
        Properties properties = new Properties();
        File f = new File(envPropertiesFile);
        if (f.exists() && !f.isDirectory()) {
            try (InputStream input = new FileInputStream(envPropertiesFile)) {
                properties.load(input);
                Class<?> c = ServiceConfiguration.class;
                Map<String, Field> fieldMap = new HashMap<>();
                Arrays.stream(c.getDeclaredFields()).forEach(x -> fieldMap.put(x.getName(), x));

                for (Object obj : properties.keySet()) {
                    String key = (String) obj;
                    String value = properties.getProperty(key);
                    if (fieldMap.containsKey(key)) {

                        Field fld = fieldMap.get(key);
                        Class<?> typeOf = fld.getType();
                        fld.setAccessible(true);
                        try {
                            if (typeOf == List.class) {
                                fld.set(this, Arrays.asList(properties.getProperty("servers").split(",")));
                            } else if (typeOf == String.class) {
                                fld.set(this, value);
                            } else if (typeOf == Integer.class) {
                                fld.set(this, Integer.valueOf(value));
                            } else if (typeOf == Boolean.class) {
                                fld.set(this, "true".equals(value));
                            } else if (typeOf == Class.class) {
                                fld.set(this, Class.forName(value));
                            }
                        } catch (Exception ex) {
                            log.error("FUBAR", ex);
                        }

                    }
                }

                setGlobalProperties();

            } catch (IOException ex) {
                log.error("Configuration file error ", ex);
            }
            log.info("================================================================");
            log.info("    KAFKA_BOOTSTRAP == {}", this.getServers());
            log.info("    SCHEMA_REGISTRY == {}", this.getRegistry_url());
            log.info("================================================================");
        }

    }

    private void setGlobalProperties() {
        if (security) {
            assert (token_url != null);
            assert (client_id != null);
            assert (client_secret != null);
            String jassConfig = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                    String.format("oauth.token.endpoint.uri=\"%s\" ", token_url) +
                    String.format("oauth.client.id=\"%s\" ", client_id) +
                    String.format("oauth.client.secret=\"%s\" ;", client_secret);

            globalProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            globalProperties.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
            globalProperties.put(SaslConfigs.SASL_JAAS_CONFIG, jassConfig);
            globalProperties.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
        }
    }

    public Properties getAdminProperties() {
        String bootStrapServers = StringUtils.join(servers, ",");
        // kafka producer setup
        Properties clientProperties = new Properties();
        clientProperties.put(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-utils");
        clientProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        clientProperties.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 10000);
        clientProperties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
        return clientProperties;
    }

    public Properties getProducerProperties(SERIALIZER serializer) {
        String bootStrapServers = StringUtils.join(servers, ",");

        Properties producerProps = new Properties();
        producerProps.putAll(globalProperties);
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-utils");
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        producerProps.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 10000);

        //--- todo ----
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
        producerProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384 * 4); // Batch up to 64K buffer in the queue.
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 100); // wait this long before de-queue regardless of queue size

        if (serializer.equals(SERIALIZER.AVRO)) {
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            producerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry_url);
            producerProps.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        } else if (serializer.equals(SERIALIZER.JSON)) {
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        } else if (serializer.equals(SERIALIZER.STRING)) {
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        }
        return producerProps;
    }

    public Properties getConsumerProperties(SERIALIZER serializer) {
        String bootStrapServers = StringUtils.join(servers, ",");

        Properties consumerProps = new Properties();
        consumerProps.putAll(globalProperties);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-utils");
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        consumerProps.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 10000);

         consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
        consumerProps.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, page_size);

        if (serializer.equals(SERIALIZER.AVRO)) {
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
            consumerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry_url);
            consumerProps.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        } else if (serializer.equals(SERIALIZER.JSON)) {
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        } else if (serializer.equals(SERIALIZER.STRING)) {
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
       }
        return consumerProps;
    }

}