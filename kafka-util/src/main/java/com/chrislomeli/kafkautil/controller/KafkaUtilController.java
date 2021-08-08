package com.chrislomeli.kafkautil.controller;

import com.chrislomeli.kafkautil.model.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import difflib.Chunk;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;


@RestController
@Slf4j
public class KafkaUtilController {

    KafkaServiceFacade kafkaService;

    @Autowired
    public KafkaUtilController(KafkaServiceFacade kafkaService) {
        this.kafkaService = kafkaService;
    }

    @GetMapping(path = "/ping")
    public String ping() {
        return kafkaService.ping();
    }

    /*
      URLS
     */
    @GetMapping(path = "/brokers")
    public List<String> getBrokers() {
        return kafkaService.getBrokers();
    }

    @GetMapping(path = "/registryUrl")
    public String getSchemaRegistryUrl() {
        return kafkaService.getSchemaRegistryUrl();
    }


    /*
      Topics
     */
    @GetMapping(path = "/cluster")
    public ClusterInfo getCluster() throws RestClientException, ExecutionException, InterruptedException {
        ClusterInfo cluster = kafkaService.describeCluster();
        if (cluster.getId().isEmpty())
            throw new RestClientException("ClusterInfo is not available", HttpStatus.SERVICE_UNAVAILABLE.value(), 500);
        return cluster;
    }

    @GetMapping(path = "/topic")
    public List<String> getTopics() {
        Optional<List<String>> topics = Optional.ofNullable(kafkaService.getTopics());
        return topics.orElse(Collections.emptyList());
    }

    @GetMapping(path = "/topic/{topic}")
    public ResponseEntity<SimpleTopicDescription> getTopicDescription(@PathVariable String topic) throws IOException, ExecutionException, InterruptedException, RestClientException {
        log.info("getTopicDescription for {}", topic);
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("Content-Type", "application/json");

        // get description
        SimpleTopicDescription std = kafkaService.getTopicDescription(topic);
        if ( std == null )
            return ResponseEntity.notFound()
                    .headers(responseHeaders)
                    .build();
        // get offsets and fold them into the description
        Collection<TopicPartitionStats> partitionsStats = kafkaService.getTopicConsumer(topic);
        for (TopicPartitionStats ps : partitionsStats) {
            int partition = ps.getPartition();
            for (TopicPartitionInfo pi : std.getPartitions()) {
                if (pi.getPartition() == partition) {
                    pi.setFirsOffset(ps.getFirstOffset());
                    pi.setLastOffset(ps.getLastOffset());
                }
            }
        }
        return ResponseEntity.ok()
                .headers(responseHeaders)
                .body(std);
    }

    @DeleteMapping(path = "/topic/{topic}")
    public ResponseEntity<String> deleteTopic(@PathVariable String topic) throws RestClientException {
        HttpHeaders responseHeaders = new HttpHeaders();
        HttpStatus status = kafkaService.deleteTopic(topic);
        return ResponseEntity.status(status)
                .headers(responseHeaders)
                .build();
    }

    @PostMapping(path = "/topic")
    public ResponseEntity<SimpleTopicDescription> postTopic(@RequestBody TopicRequest topicInfo) throws ExecutionException, InterruptedException, RestClientException {
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("Content-Type", "application/json");
        // create
        kafkaService.saveTopic(topicInfo);
        // created location
        URI location = URI.create(String.format("/topic/%s", topicInfo.getTopic()));
        // read it back
        SimpleTopicDescription simple =  kafkaService.getTopicDescription(topicInfo.getTopic());
        // return
        return ResponseEntity.created(location)
                .headers(responseHeaders)
                .body(simple);
    }

    @PutMapping(path = "/topic/{topic}/strings")
    public ResponseEntity<Map<String,Integer>> writeToTopic(@PathVariable String topic, @RequestBody List<String> lines) {
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("Content-Type", "application/json");

        Map<String,Integer> results = kafkaService.sendStringRecordsToTopic(topic, lines);
        return ResponseEntity.ok()
                .headers(responseHeaders)
                .body(results);
    }

    @GetMapping(path = "/topic/{topic}/records/{partition}/{offset}")
    public ResponseEntity<List<Map<String, Object>>> getTopicDataByOffset(
            @PathVariable String topic,
            @PathVariable int partition,
            @PathVariable long offset)  {
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("Content-Type", "application/json");

        List<Map<String, Object>> json = kafkaService.getTopicDataByOffset(topic,partition,offset);
        return ResponseEntity.ok()
                .headers(responseHeaders)
                .body(json);
    }

    @PutMapping(path = "/topic/{topic}/record")
    public ResponseEntity<String> sendRecord(@PathVariable String topic, @RequestBody String record) throws RestClientException, JsonProcessingException {
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("Content-Type", "application/json");
        // create
        String responseString = kafkaService.sendAvroRecordsToTopic(topic, record);
        // return
        return ResponseEntity.ok()
                .headers(responseHeaders)
                .body(responseString);
    }


    @GetMapping(path = "/topicDate/{topic}/{timestamp}")
    public ResponseEntity<List<Map<String, Object>>> getTopicDataByDate(
            @PathVariable String topic, @PathVariable String timestamp ) throws IOException, RestClientException {

        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("Content-Type", "application/json");

        List<Map<String, Object>> json = kafkaService.getTopicDataByDate(topic, timestamp);
        return ResponseEntity.ok()
                .headers(responseHeaders)
                .body(json);
    }

    @GetMapping(path = "/topicTimestamp/{topic}/{timestamp}")
    public ResponseEntity<List<Map<String, Object>>> getTopicDataByDateTime(
            @PathVariable String topic, @PathVariable long timestamp )  {

        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("Content-Type", "application/json");
        List<Map<String, Object>> json = kafkaService.getTopicDataByDateTime(topic, timestamp);
        return ResponseEntity.ok()
                .headers(responseHeaders)
                .body(json);
    }


    /*

    Subject

     */
    @GetMapping(path = "/subject")
    public ResponseEntity<List<String>> getSchemas() throws IOException {
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("Content-Type", "application/json");
        Optional<List<String>> subjects =  kafkaService.getSubjects();

        if (subjects.isEmpty())
            return ResponseEntity.notFound()
                    .headers(responseHeaders)
                    .build();
        else
            return ResponseEntity.ok()
                    .headers(responseHeaders)
                    .body(subjects.get());

    }

    @GetMapping(path = "/subject/{topic}/versions")
    public ResponseEntity<List<Integer>> getSchemaVersions(@PathVariable String topic) throws IOException, RestClientException {
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("Content-Type", "application/json");

        Optional<List<Integer>> schema = kafkaService.getAllVersions(topic);
        if (schema.isEmpty())
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, String.format("Schema for topic %s  is not Found", topic));
        else
            return ResponseEntity.ok()
                    .headers(responseHeaders)
                    .body(schema.get());
    }

    @GetMapping(path = "/subject/version/{id}")
    public ResponseEntity<String> getSchemaById(@PathVariable Integer id) throws IOException, RestClientException {
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("Content-Type", "application/json");

        Optional<String> schema = kafkaService.getSchemaById(id);
        if (schema.isEmpty())
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, String.format("Schema for id %d  is not Found", id));
        else
            return ResponseEntity.ok()
                    .headers(responseHeaders)
                    .body(schema.get());
    }

    @DeleteMapping(path = "/subject/{topic}")
    public ResponseEntity<String> deleteSchemas(@PathVariable String topic) throws IOException, RestClientException {
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("Content-Type", "application/json");

        Optional<String> deleted = kafkaService.deleteSchemas(topic);
        if (deleted.isEmpty())
            return ResponseEntity.notFound()
                    .headers(responseHeaders)
                    .build();
        else
            return ResponseEntity.ok()
                    .headers(responseHeaders)
                    .body(deleted.get());
    }

    /* Schema */
    @GetMapping(path = "/schema/{topic}")
    public ResponseEntity<String> getSchemaByTopic(@PathVariable String topic) throws IOException, RestClientException {
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("Content-Type", "application/json");

        Optional<String> schema = kafkaService.findSubject(topic);
        if (schema.isEmpty())
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, String.format("Schema for topic %s  is not Found", topic));
        else
            return ResponseEntity.ok()
                    .headers(responseHeaders)
                    .body(schema.get());
    }

    @PostMapping(path = "/schema/{topic}")
    public ResponseEntity<String> saveSchemas(@PathVariable String topic, @RequestBody String schema) throws IOException, RestClientException {
        log.info("Create subject for topic {}", topic);
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("Content-Type", "application/json");
        // create
        Optional<Integer> offset = kafkaService.saveSubject(topic, schema);
        Optional<String> schemaOption = Optional.empty();
        URI location;
        if (offset.isEmpty())
            throw new ResponseStatusException(
                    HttpStatus.NOT_FOUND, String.format("Schema for topic %s  is not Found", topic));

        // created location
        location = URI.create(String.format("/subject/%s?id=%d", topic, offset.get()));
        // return
        schemaOption = kafkaService.findSubject(topic);
        if (schemaOption.isEmpty())
            return ResponseEntity.notFound()
                    .headers(responseHeaders)
                    .build();
        else
            return ResponseEntity.created(location)
                    .headers(responseHeaders)
                    .body(schemaOption.get().toString());
    }

    @GetMapping(path = "/schema/{topic}/id")
    public Optional<String> getSchemaById(int id) throws IOException, RestClientException {
        return kafkaService.getSchemaById(id);
    }

    @GetMapping(path = "/schema/{topic}/validate")
    public boolean isSchemaCompatible(@PathVariable String topic, @RequestBody String schemaString) throws IOException, RestClientException {
        return kafkaService.isSchemaCompatible(topic,schemaString);
    }

    @GetMapping(path = "/schema/{topic}/config")
    public ResponseEntity<Config> getConfig(@PathVariable String topic) throws IOException, RestClientException {
        return ResponseEntity.ok()
                .body(kafkaService.getConfig(topic));
    }

    @PutMapping(path = "/schema/{topic}/compatibility/{compatibility}")
    public ResponseEntity<String> updateCompatibility(@PathVariable String topic,@PathVariable String compatibility) throws IOException, RestClientException {
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.set("Content-Type", "application/json");
        String diffs =  kafkaService.updateCompatibility(topic, compatibility);
        return ResponseEntity.ok()
                .headers(responseHeaders)
                .body(diffs);
    }

    @GetMapping(path = "/schema/{topic}/compare")
    public ResponseEntity<List<Chunk<String>>> compareSchema(@PathVariable String topic, @RequestBody String schemaString) throws IOException, RestClientException {
        List<Chunk<String>> diffs =  kafkaService.compareSchema(topic, schemaString);
        return ResponseEntity.ok()
                .body(diffs);
    }
}

