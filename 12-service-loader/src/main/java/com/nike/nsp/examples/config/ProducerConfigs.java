package com.nike.nsp.examples.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Properties;

public class ProducerConfigs {

    private static final Logger log = LogManager.getLogger(ProducerConfigs.class);

    private final String bootstrapServers;
    private final String topic;
    private final String trustStorePassword;
    private final String trustStorePath;
    private final String keyStorePassword;
    private final String keyStorePath;
    private final String oauthClientId;
    private final String oauthClientSecret;
    private final String oauthTokenEndpointUri;
    private final String valueSerializer;
    private final String schemaRegistryUri;
    private final String oauthToken;
    private String acks = "1";

    public ProducerConfigs(String bootstrapServers, String topic, String trustStorePassword, String trustStorePath, String keyStorePassword, String keyStorePath, String oauthClientId, String oauthClientSecret, String oauthTokenEndpointUri, String valueSerializer, String schemaRegistryUri, String oauthToken) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.trustStorePassword = trustStorePassword;
        this.trustStorePath = trustStorePath;
        this.keyStorePassword = keyStorePassword;
        this.keyStorePath = keyStorePath;
        this.oauthClientId = oauthClientId;
        this.oauthClientSecret = oauthClientSecret;
        this.oauthTokenEndpointUri = oauthTokenEndpointUri;
        this.valueSerializer = valueSerializer;
        this.schemaRegistryUri = schemaRegistryUri;
        this.oauthToken = oauthToken;
    }

    public static ProducerConfigs fromEnv() {
        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        String topic = System.getenv("TOPIC");
        String trustStorePassword = System.getenv("TRUSTSTORE_PASSWORD");
        String trustStorePath = System.getenv("TRUSTSTORE_PATH");
        String keyStorePassword = System.getenv("KEYSTORE_PASSWORD");
        String keyStorePath = System.getenv("KEYSTORE_PATH");
        String oauthClientId = System.getenv("OAUTH_CLIENT_ID");
        String oauthClientSecret = System.getenv("OAUTH_CLIENT_SECRET");
        String oauthTokenEndpointUri = System.getenv("OAUTH_TOKEN_ENDPOINT_URI");
        String valueSerializer = System.getenv("VALUE_SERIALIZER");
        String schemaRegistryUri = System.getenv("SCHEMA_REGISTRY_URI");
        String oauthToken = System.getenv("OAUTH_TOKEN");

        return new ProducerConfigs(bootstrapServers, topic, trustStorePassword, trustStorePath, keyStorePassword, keyStorePath, oauthClientId, oauthClientSecret, oauthTokenEndpointUri, valueSerializer, schemaRegistryUri, oauthToken);
    }

    public static Properties createProperties(ProducerConfigs config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, config.getAcks());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //props.put(ProducerConfigs.MAX_REQUEST_SIZE_CONFIG, "" + 1024 * 1024 * 10);

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistryUri());
        props.put(AbstractKafkaSchemaSerDeConfig.BEARER_AUTH_CREDENTIALS_SOURCE,"SASL_INHERIT");
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,false);
//        props.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"SASL_INHERIT");

        props.put(AbstractKafkaSchemaSerDeConfig.REQUEST_HEADER_PREFIX + "Authorization", "Bearer " + config.getOauthToken());


        if (config.getTrustStorePassword() != null && config.getTrustStorePath() != null) {
            log.info("Configuring truststore");
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, config.getTrustStorePassword());
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, config.getTrustStorePath());

        }

        if (config.getKeyStorePassword() != null && config.getKeyStorePath() != null) {
            log.info("Configuring keystore");
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, config.getKeyStorePassword());
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, config.getKeyStorePath());
        }

        if (config.getOauthTokenEndpointUri() != null && config.getOauthClientId() != null && config.getOauthClientSecret() != null) {

            String jassConfig = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                    String.format("oauth.token.endpoint.uri=\"%s\" ", config.oauthTokenEndpointUri) +
                    String.format("oauth.client.id=\"%s\" ", config.oauthClientId) +
                    String.format("oauth.client.secret=\"%s\" ;", config.oauthClientSecret);
            props.put(SaslConfigs.SASL_JAAS_CONFIG, jassConfig);



//            props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            props.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
            props.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
        }

        return props;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public String getAcks() {
        return acks;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public String getTrustStorePath() {
        return trustStorePath;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public String getKeyStorePath() {
        return keyStorePath;
    }

    public String getOauthClientId() {
        return oauthClientId;
    }

    public String getOauthClientSecret() {
        return oauthClientSecret;
    }

    public String getOauthTokenEndpointUri() {
        return oauthTokenEndpointUri;
    }

    public String getValueSerializer() {
        return valueSerializer;
    }

    public String getSchemaRegistryUri() {
        return schemaRegistryUri;
    }

    public String getOauthToken() { return oauthToken; }
}

