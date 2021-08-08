package com.chrislomeli.springsandbox.producer;

import com.chrislomeli.springsandbox.generic.GenericRecordProvider;
import com.chrislomeli.springsandbox.model.NotificationStatus;
import com.chrislomeli.springsandbox.model.ServiceStatus;
import com.chrislomeli.springsandbox.registry.DefaultBearerAuthCredentialProvider;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableAsync
@Slf4j
public class ProducerConfiguration {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value(value = "${notification_status_topic.schema}")
    private String notificationStatusSchemaFile;

    @Value(value = "${notification_status_topic.name}")
    private String notificationStatusTopic;

    @Value(value = "${service_status_topic.schema}")
    private String serviceStatusSchemaFile;

    @Value(value = "${service_status_topic.schema}")
    private String serviceStatusTopic;

    @Value(value = "${app.security}")
    private String security;

    @Value(value = "${app.client_secret}")
    private String client_secret;

    @Value(value = "${app.client_id}")
    private String client_id;

    @Value(value = "${app.token_url}")
    private String token_url;

    @Value(value = "${spring.kafka.properties.schema.registry.url}")
    private String registry_url;

    /* this is an alternative to the avcs file where we just construct the scema from a json
      since we are using generic records the only additional thing we have to to is set the schema on the generic record
      we have two topics so we create a map of schemas - this is not really a bean, we just want to do it up front - so move this logic
      somewhere else
    */
    @Bean
    public void setSchemas()  {
        try {
            GenericRecordProvider.setSchemaFromFile(notificationStatusTopic, NotificationStatus.class, notificationStatusSchemaFile);
            GenericRecordProvider.setSchemaFromFile(serviceStatusTopic, ServiceStatus.class, serviceStatusSchemaFile);
        } catch (Exception e) {
            log.error("Cannot load schema from json", e);
        }
    }

    /*
      separate configuration from instantiation
     */
    @Bean
    public Map<String, Object> producerConfigs() throws ClassNotFoundException {

        Map<String, Object> producerProperties = new HashMap<>();
        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "appid");
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProperties.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 10000);
        producerProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        producerProperties.put(ProducerConfig.RETRIES_CONFIG, 3);
        producerProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
        producerProperties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        producerProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384 * 2); // Batch up to 64K buffer in the queue.
        producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, 40000); // wait this long before de-queue regardless of queue size

        producerProperties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registry_url);
        producerProperties.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        producerProperties.put(AbstractKafkaAvroSerDeConfig.BEARER_AUTH_CREDENTIALS_SOURCE, DefaultBearerAuthCredentialProvider.TOKEN_PROVIDER_NAME);  // kafka uses a service loader to get the bearer auth when calling the schema registry

        if ("true".equals(security)) {
            String jassConfig = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                    String.format("oauth.token.endpoint.uri=\"%s\" ", token_url) +
                    String.format("oauth.client.id=\"%s\" ", client_id) +
                    String.format("oauth.client.secret=\"%s\" ;", client_secret);
            producerProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            producerProperties.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
            producerProperties.put(SaslConfigs.SASL_JAAS_CONFIG, jassConfig);
            producerProperties.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
        }
        return producerProperties;
    }

    @Bean
    public ProducerFactory<String, GenericRecord> kafkaProducerFactory() throws ClassNotFoundException {
        return new DefaultKafkaProducerFactory<String, GenericRecord>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, GenericRecord> kafkaTemplate() throws ClassNotFoundException {
        return new KafkaTemplate<>(kafkaProducerFactory());
    }

}
