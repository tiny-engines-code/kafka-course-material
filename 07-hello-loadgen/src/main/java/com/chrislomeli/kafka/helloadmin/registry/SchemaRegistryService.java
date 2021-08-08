package com.chrislomeli.kafka.helloadmin.registry;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ModeGetResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Optional;

@Slf4j
public class SchemaRegistryService {
    RestService client;

    public SchemaRegistryService(String registryUrl) {
        client = new RestService(registryUrl);
    }

    public void poll(String topic) {
        log.info("Connect to Schema Registry...");
        while (!serviceAvailable()) {
            log.error("Retry connecting to Schema Registry ...");
            try {
                Thread.sleep(2000);
            } catch (Exception ignored) {
            }
        }
    }

    public Optional<Integer> saveSubject(String topic, String type, String schemaInput) throws IOException, RestClientException {
        String subject = String.format("%s-%s", topic, type);
        String data1 = schemaInput.replaceAll("\n", "");
        int schemaId = client.registerSchema(data1, subject);
        return Optional.of(schemaId);
    }

    public Optional<Schema> serviceReady(String topic) {
        log.info("Connect to Schema Registry...");
        if (!serviceAvailable()) return Optional.empty();
        log.info("Connected to Schema Registry!!!");

        String subject = String.format("%s-value", topic);
        Optional<Schema> schema = getSchema(subject);
        if (schema.isEmpty())
            log.error("Schema {} is not found", subject);
        return schema;
    }

    public boolean serviceAvailable() {
        Optional<String> mode = getMode();
        if (mode.isPresent()) {
            if (!"READWRITE".equals(mode.get())) {
                log.error("Schema registry at {} is not in READWRITE mode !!!", client.getBaseUrls());
                return false;
            }
            return true;
        }
        log.error("Schema registry at {} is not available !!!", client.getBaseUrls());
        return false;
    }

    public Optional<String> getMode() {
        try {
            ModeGetResponse s = client.getMode();
            return Optional.ofNullable(s.getMode());
        } catch (IOException | RestClientException ex) {
            return Optional.empty();
        }
    }

    public Optional<Schema> getSchema(String subject) {
        try {
            String sc = client.getLatestVersionSchemaOnly(subject);
            Schema.Parser sp = new Schema.Parser();
            Schema schema = sp.parse(sc);
            return Optional.ofNullable(schema);

        } catch (IOException | RestClientException e) {
            return Optional.empty();
        }
    }

    public void isSchemaCompatible(String topic, Schema schema) throws IOException, RestClientException {
        String schemaString = schema.toString();
        String subject = String.format("%s-value", topic);
        boolean compatible = client.testCompatibility(schemaString, String.format("%s-value", topic), "latest");
        if (!compatible) {
            String registeredSchema = client.getLatestVersionSchemaOnly(subject);
            log.error("WTF");
        }
    }
}
