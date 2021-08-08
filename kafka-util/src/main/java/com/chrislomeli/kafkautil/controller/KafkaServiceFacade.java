package com.chrislomeli.kafkautil.controller;

import com.chrislomeli.kafkautil.ServiceConfiguration;
import com.chrislomeli.kafkautil.kafka.*;
import com.chrislomeli.kafkautil.model.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import difflib.Chunk;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Slf4j
@Component
public class KafkaServiceFacade {

    static final ObjectMapper mapper = new ObjectMapper();

    ServiceConfiguration myConfig;
    KafkaAdminService admin;
    SchemaClientService schemaRegistryClient;


    public KafkaServiceFacade() {
        this.myConfig = ServiceConfiguration.getInstance();
        this.schemaRegistryClient = SchemaClientService.getInstance();
        this.admin = new KafkaAdminService();
    }

    public String ping() {
        return "Pong";
    }

    /* ClusterInfo */
    public ClusterInfo describeCluster() throws ExecutionException, InterruptedException {
        return admin.describeCluster();
    }

    /*
      URLS
     */
    public List<String> getBrokers() {
        return myConfig.getServers();
    }

    public String getSchemaRegistryUrl() {
        return myConfig.getRegistry_url();
    }

    /*
      TOPIC
     */
    public List<String> getTopics() {
        Optional<List<String>> topics = admin.listTopics();
        return topics.orElse(Collections.emptyList());
    }

    public SimpleTopicDescription getTopicDescription(String topic) throws ExecutionException, InterruptedException, RestClientException {
        Optional<SimpleTopicDescription> description = admin.describeTopic(topic);
        if (description.isEmpty())
            throw new RestClientException(String.format("topic %s not found",topic ),  HttpStatus.NOT_FOUND.value(), 400);

        SimpleTopicDescription topicDescription = description.get();
        try {
            topicDescription.setHasSchema(schemaRegistryClient.subjectExists(topic, "value"));
        } catch (IOException ignored) {
        }
        return topicDescription;
    }

    public void saveTopic(TopicRequest topic) throws ExecutionException, InterruptedException {
        admin.saveTopic(topic);
    }

    public HttpStatus deleteTopic(String topic) throws RestClientException {
        return admin.deleteTopic(topic);
    }

    public Collection<TopicPartitionStats> getTopicConsumer(String topic) throws IOException {
        try (KafkaConsumerService<String, GenericRecord> consumerUtil =
                     new KafkaConsumerService<>(myConfig.getConsumerProperties(ServiceConfiguration.SERIALIZER.STRING))) {

            return consumerUtil.getPartitionInfo(topic);
        }
    }

    public Map<String,Integer> sendStringRecordsToTopic(String topic, List<String> lines) {
        try (KafkaProducerService<String> producerUtil = new KafkaProducerService<>(myConfig.getProducerProperties(ServiceConfiguration.SERIALIZER.STRING))) {
            return producerUtil.sendStringRecords(topic, lines);
        }
    }

    public String sendAvroRecordsToTopic(String topic, String jsonString) throws RestClientException, JsonProcessingException {
        // get the schema for this topic
        Optional<String> schemaString = Optional.empty();
        try {
            schemaString = schemaRegistryClient.findSubject(topic, "value");
        } catch (IOException ignored) {
        }
        if (schemaString.isEmpty())
            throw new RestClientException(String.format("Schema for %s not found ", topic), HttpStatus.NOT_FOUND.value(), 400);

        // transform json to a Gerneric record
        GenericRecordServiceResponse response = GenericRecordService.transform(schemaString.get(), jsonString);
        GenericRecord genericRecord = response.getGenericRecord();

        // Kafka producer wrapper
        try (KafkaProducerService<GenericRecord> producerUtil =
                     new KafkaProducerService<>(myConfig.getProducerProperties(ServiceConfiguration.SERIALIZER.AVRO))) {

            producerUtil.sendRecord(topic, genericRecord);

            Map<String, Object> out = new HashMap<>();
            out.put("result", "Success");
            out.put("schemaFieldsNotPopulated", response.getUnpopulatedSchemaFields());
            out.put("extraFieldsNotUsed", response.getUnusedDataFields());
            return mapper.writeValueAsString(out);

        } catch (ExecutionException | InterruptedException | JsonProcessingException e) {
            log.error("Execution error while sending to topic {}", topic, e);
            throw new RuntimeException(
                    String.format("Failed to send to topic %s ", topic));
        }
    }

    public List<Map<String, Object>> getTopicDataByOffset(
            String topic,
            int partition,
            long offset) {

        ServiceConfiguration.SERIALIZER serializerType = ServiceConfiguration.SERIALIZER.STRING;
        try {
            if (schemaRegistryClient.subjectExists(topic, "value"))
                serializerType = ServiceConfiguration.SERIALIZER.AVRO;
        } catch (Exception ignored) {
        }

        try (KafkaConsumerService<String, GenericRecord> consumerUtil =
                     new KafkaConsumerService<>(myConfig.getConsumerProperties(serializerType))) {

            return consumerUtil.getPageByOffset(topic, partition, offset);
        }
    }

    public List<Map<String, Object>> getTopicDataByDateTime(
            String topic, long timestamp) {

        ServiceConfiguration.SERIALIZER serializerType = ServiceConfiguration.SERIALIZER.STRING;
        try {
            if (schemaRegistryClient.subjectExists(topic, "value"))
                serializerType = ServiceConfiguration.SERIALIZER.AVRO;
        } catch (Exception ignored) {
        }

        try (KafkaConsumerService<String, GenericRecord> consumerUtil =
                     new KafkaConsumerService<>(myConfig.getConsumerProperties(serializerType))) {

            return consumerUtil.getPageByTimestamp(topic, timestamp);
        }
    }

    public List<Map<String, Object>> getTopicDataByDate(String topic, String dateTimeString) throws IOException, IllegalArgumentException, RestClientException {
        DateTime targetDate;
        try {
            DateTimeFormatter fmt = DateTimeFormat.forPattern("MM-dd-yyyy:HH:mm:ss");
            targetDate = fmt.parseDateTime(dateTimeString);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Date format should be: MM-dd-yyy:HH:mm:ss");
        }

        ServiceConfiguration.SERIALIZER serializerType = ServiceConfiguration.SERIALIZER.STRING;
        if (schemaRegistryClient.subjectExists(topic, "value"))
            serializerType = ServiceConfiguration.SERIALIZER.AVRO;
        try (KafkaConsumerService<String, GenericRecord> consumerUtil =
                     new KafkaConsumerService<>(myConfig.getConsumerProperties(serializerType))) {
            return consumerUtil.getPageByTimestamp(topic, targetDate.getMillis());
        }
    }


    /*  SCHEMA */
    public Optional<List<String>> getSubjects()  {
        return schemaRegistryClient.getSubjects();
    }

    public Optional<String> findSubject(String topic) throws IOException, RestClientException {
        return schemaRegistryClient.findSubject(topic, "value");
    }

    public Optional<Integer> saveSubject(String topic, String schema) throws IOException, RestClientException {
        return schemaRegistryClient.saveSubject(topic, "value", schema);
    }

    public Optional<String> deleteSchemas(String topic) throws IOException, RestClientException {
        if (!schemaRegistryClient.subjectExists(topic, "value")) {
            log.info("subject to delete: {}-value is not found", topic);
            return Optional.empty();
        }
        Optional<List<Integer>> deleted = schemaRegistryClient.deleteSubject(topic, "value", true);
        if (deleted.isPresent()) {
            StringBuffer sb = new StringBuffer("{\"deleted\": [");
            deleted.get().forEach(x -> sb.append(x).append(","));
            sb.append("]}");
            return Optional.of(sb.toString());
        }
        log.info("subject to delete: {}-value - failed to get offsets back - deleted?", topic);
        return Optional.empty();
    }

    public Optional<List<Integer>> getAllVersions(String topic) throws IOException, RestClientException {
        return schemaRegistryClient.getAllVersions(topic, "value");
    }

    public Optional<String> getSchemaById(int id) throws IOException, RestClientException {
        return schemaRegistryClient.getSchemaById(id);
    }

    public boolean isSchemaCompatible(String topic, String schemaString) throws IOException, RestClientException {
        return schemaRegistryClient.isSchemaCompatible(topic,schemaString);
    }

    public Config getConfig(String topic) throws IOException, RestClientException {
        return schemaRegistryClient.getConfig(topic);
    }

    public String updateCompatibility(String topic, String compat) throws IOException, RestClientException {
        return schemaRegistryClient.updateCompatibility(topic, compat);
    }


    public List<Chunk<String>> compareSchema(String topic, String schemaString) throws IOException, RestClientException {
        List<Chunk<String>> diffs =  schemaRegistryClient.compareSchema(topic, schemaString);
        return diffs;
    }
}
