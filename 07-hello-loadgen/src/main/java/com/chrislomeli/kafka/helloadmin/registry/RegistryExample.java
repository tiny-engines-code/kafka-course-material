package com.chrislomeli.kafka.helloadmin.registry;

import com.chrislomeli.kafka.helloadmin.config.MyConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static java.lang.Thread.sleep;

@Slf4j
public class RegistryExample {

    public SchemaRegistryService registryService;

    public RegistryExample(String registry_url) {
        registryService = new SchemaRegistryService(registry_url);
    }

    public Schema getLocalSchemaFromFile(String fileName) throws IOException {
        String data = "";
        data = new String(Files.readAllBytes(Paths.get(fileName)));
        String schemaString = data.replaceAll("\n", "");
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    public static Optional<String> doExample(String topic, Class<?> clazz, String file) {

        try {
            RegistryExample registryExample = new RegistryExample(MyConfiguration.registry_url);

            // poll until we can connect to the registry service ?
            registryExample.registryService.poll(topic);

            // if you have a local schema you can verify it against the remote schema - this is hard coded to the User class fields
            Schema localSchema = registryExample.getLocalSchemaFromFile(file);

            // but in this case we want to register it because it does not exist. normally we don't create the schema in production producer code - so this is just a way of bootstrapping the registryExample
            Optional<Integer> id = registryExample.registryService.saveSubject(topic, "value", localSchema.toString());
            if (id.isEmpty())
                throw new RuntimeException(String.format("Can't create a value schema for topic %s", topic));

            // you can verify a local schema against the remote one - in this case we just created it - so it should match
            // but if you did want to use a local schema for any reason, you can verify it against the latest remote version this way
            registryExample.registryService.isSchemaCompatible(topic, localSchema);

            // We can validate the class itself against either the localSchema or the remote one - in this case we are using the remote schema
            // first get the remote schema
            String subject = String.format("%s-value", topic); // just get the value, no the key and assume that we bind to the topic by name
            Optional<Schema> remoteSchemaOption = registryExample.registryService.getSchema(subject);
            if (remoteSchemaOption.isEmpty())
                throw new RuntimeException(String.format("Can't find a schema with name %s", subject));

            // validate the schema we retrieved against the class passed in
            Schema remoteSchema = remoteSchemaOption.get();
            SchemaValidatorStats stats = SchemaValidatorService.validateClass(remoteSchema, clazz);
            // just a quick and dirty validation for registryExample
            assert (0 < stats.getCompatibleFields().size());
            assert (0 == stats.getIncompatibleFields().size());
            assert (0 == stats.getUnpopulatedSchemaFields().size());
            assert (0 == stats.getUnusedDataFields().size());

            return Optional.of(remoteSchema.toString());

        } catch (Exception e) {
            log.error("Registry example failed", e);
            return Optional.empty();
        }

    }


}
