package com.chrislomeli.kafkautil.kafka;

import com.chrislomeli.kafkautil.ServiceConfiguration;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import difflib.Chunk;
import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.*;

@Slf4j
@SuperBuilder
public class SchemaClientService {

    private static SchemaClientService schemaClientService;

    RestService client;
    String base64Encoded;
    static ObjectMapper mapper = new ObjectMapper();

    public static SchemaClientService getInstance() {
        if (schemaClientService == null)
            schemaClientService =  new SchemaClientService();
        return schemaClientService;
    }

    public SchemaClientService() {
        ServiceConfiguration serviceConfiguration = ServiceConfiguration.getInstance();
        client = new RestService(serviceConfiguration.getRegistry_url());
     }

    public Optional<List<String>> getSubjects()  {
        try {
            return Optional.of( client.getAllSubjects() );

        } catch (IOException | RestClientException ex) {
            log.error("Failed to get subjects: ", ex);
            return Optional.empty();
        }
    }

    public boolean subjectExists(String topic, String type) throws IOException, RestClientException {
        String subject = String.format("%s-%s", topic, type);
        List<String> subjects =  client.getAllSubjects();
        return subjects.contains(subject);
    }

    public Optional<String> findSubject(String topic, String type) throws IOException, RestClientException {
        String subject = String.format("%s-%s", topic, type);
            String sc = client.getLatestVersionSchemaOnly(subject);
            return Optional.of(sc);
    }

    public Optional<List<Integer>> deleteSubject(String topic, String type, boolean permanent) throws IOException, RestClientException {
        String subject = String.format("%s-%s", topic, type);
        Map<String,String> props = new HashMap<>();
        if (permanent)
            props.put("permanent", "true");
        return Optional.of(client.deleteSubject(props, subject));
    }

    public Optional<Integer> saveSubject(String topic, String type, String schemaInput) throws IOException, RestClientException {
        String subject = String.format("%s-%s", topic, type);
        String data1 = schemaInput.replaceAll("\n", "");
        int schemaId = client.registerSchema(data1, subject);
        return Optional.of(schemaId);
    }

    public UrlList getBaseUrls() {
        return  client.getBaseUrls();
    }

    public Optional<String> updateCompatibility(String topic, String type, String compatibility) throws IOException, RestClientException {
        String subject = String.format("%s-%s", topic, type);
        ConfigUpdateRequest request = client.updateCompatibility(compatibility, subject);
        return Optional.of(request.toJson());
   }

    public Optional<List<Integer>> getAllVersions(String topic, String type) throws IOException, RestClientException {
        String subject = String.format("%s-%s", topic, type);
        List<Integer> versions = client.getAllVersions(subject);
        return Optional.of(versions);
    }

    public Optional<String> getSchemaById(int id) throws IOException, RestClientException {
        SchemaString schemaString = client.getId(id);
        return Optional.of(schemaString.getSchemaString());
    }


    public Optional<Integer> deleteSubjectByVersion(String topic, String type, String ver, boolean permanent) throws IOException, RestClientException {
        String subject = String.format("%s-%s", topic, type);
        Map<String,String> props = new HashMap<>();
        if (permanent)
            props.put("permanent", "true");
        return Optional.of(client.deleteSchemaVersion(props, subject, ver));
    }

    public boolean isSchemaCompatible(String topic, String schemaString) throws IOException, RestClientException {
        String subject = String.format("%s-value", topic);
        boolean compatible = client.testCompatibility(schemaString, String.format("%s-value", topic), "latest");
        if (! compatible ) {
            String registeredSchema = client.getLatestVersionSchemaOnly(subject);
            log.error("WTF");
        }
        return compatible;
    }

    public Config getConfig(String topic) throws IOException, RestClientException {
        String subject = String.format("%s-value", topic);
        Config config = client.getConfig(topic);
        return config;
    }

    public String updateCompatibility(String topic, String compat) throws IOException, RestClientException {
        String subject = String.format("%s-value", topic);
        ConfigUpdateRequest request = client.updateCompatibility(subject, compat);
        return request.toJson();
    }

    public List<Chunk<String>> compareSchema(String topic, String schemaString) throws IOException, RestClientException {
        String subject = String.format("%s-value", topic);
        boolean compatible = client.testCompatibility(schemaString, String.format("%s-value", topic), "latest");
        if (compatible) {
            Chunk<String> c = new Chunk<>(0, Collections.singletonList("No differnences"));
            return Collections.singletonList(c);
        }
        String registeredSchema = client.getLatestVersionSchemaOnly(subject);

        Map<String,Object> compareMap = mapper.readValue(schemaString, new TypeReference<Map<String, Object>>() {});
        Map<String,Object> registeredMap = mapper.readValue(registeredSchema, new TypeReference<Map<String, Object>>() {});

        String j1 =mapper.writerWithDefaultPrettyPrinter().writeValueAsString(compareMap);
        String j2 =mapper.writerWithDefaultPrettyPrinter().writeValueAsString(registeredMap);

        List<String> compareLines = Arrays.asList(j1.split("\n"));
        List<String> registeredLines = Arrays.asList(j2.split("\n"));



        List<String> registeredJson = Arrays.asList(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(registeredSchema).split("\n"));

//        final List<String> registeredLines = Arrays.asList(schemaString.split(",")).stream().map(x -> x.replaceAll("\n", "").replaceAll(" ","")).collect(Collectors.toList());
//        final List<String> compareLines = Arrays.asList(registeredSchema.split(",")).stream().map(x -> x.replaceAll("\n", "").replaceAll(" ","")).collect(Collectors.toList());;
        List<Chunk<String>> diffs = new ArrayList<>();

        final Patch<String> patch = DiffUtils.diff(registeredLines, compareLines);

        for (Delta<String> delta : patch.getDeltas() ){
            diffs.add( delta.getRevised());
        }
        return diffs;
    }

}
