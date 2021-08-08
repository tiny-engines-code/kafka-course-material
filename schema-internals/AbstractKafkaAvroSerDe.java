//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package io.confluent.kafka.serializers;

import io.confluent.common.config.ConfigException;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.errors.SerializationException;

public abstract class AbstractKafkaAvroSerDe {
    protected static final byte MAGIC_BYTE = 0;
    protected static final int idSize = 4;
    private static final String MOCK_URL_PREFIX = "mock://";
    protected SchemaRegistryClient schemaRegistry;
    protected Object keySubjectNameStrategy = new TopicNameStrategy();
    protected Object valueSubjectNameStrategy = new TopicNameStrategy();

    public AbstractKafkaAvroSerDe() {
    }

    protected void configureClientProperties(AbstractKafkaAvroSerDeConfig config) {
        List<String> urls = config.getSchemaRegistryUrls();
        int maxSchemaObject = config.getMaxSchemasPerSubject();
        Map<String, Object> originals = config.originalsWithPrefix("");
        if (null == this.schemaRegistry) {
            String mockScope = validateAndMaybeGetMockScope(urls);
            if (mockScope != null) {
                this.schemaRegistry = MockSchemaRegistry.getClientForScope(mockScope);
            } else {
                this.schemaRegistry = new CachedSchemaRegistryClient(urls, maxSchemaObject, originals, config.requestHeaders());
            }
        }

        this.keySubjectNameStrategy = config.keySubjectNameStrategy();
        this.valueSubjectNameStrategy = config.valueSubjectNameStrategy();
    }

    private static String validateAndMaybeGetMockScope(List<String> urls) {
        List<String> mockScopes = new LinkedList();
        Iterator var2 = urls.iterator();

        while(var2.hasNext()) {
            String url = (String)var2.next();
            if (url.startsWith("mock://")) {
                mockScopes.add(url.substring("mock://".length()));
            }
        }

        if (mockScopes.isEmpty()) {
            return null;
        } else if (mockScopes.size() > 1) {
            throw new ConfigException("Only one mock scope is permitted for 'schema.registry.url'. Got: " + urls);
        } else if (urls.size() > mockScopes.size()) {
            throw new ConfigException("Cannot mix mock and real urls for 'schema.registry.url'. Got: " + urls);
        } else {
            return (String)mockScopes.get(0);
        }
    }

    protected String getSubjectName(String topic, boolean isKey, Object value, Schema schema) {
        Object subjectNameStrategy = this.subjectNameStrategy(isKey);
        return subjectNameStrategy instanceof SubjectNameStrategy ? ((SubjectNameStrategy)subjectNameStrategy).subjectName(topic, isKey, schema) : ((io.confluent.kafka.serializers.subject.SubjectNameStrategy)subjectNameStrategy).getSubjectName(topic, isKey, value);
    }

    protected boolean isDeprecatedSubjectNameStrategy(boolean isKey) {
        Object subjectNameStrategy = this.subjectNameStrategy(isKey);
        return !(subjectNameStrategy instanceof SubjectNameStrategy);
    }

    private Object subjectNameStrategy(boolean isKey) {
        return isKey ? this.keySubjectNameStrategy : this.valueSubjectNameStrategy;
    }

    protected String getOldSubjectName(Object value) {
        if (value instanceof GenericContainer) {
            return ((GenericContainer)value).getSchema().getName() + "-value";
        } else {
            throw new SerializationException("Primitive types are not supported yet");
        }
    }

    public int register(String subject, Schema schema) throws IOException, RestClientException {
        return this.schemaRegistry.register(subject, schema);
    }

    public Schema getById(int id) throws IOException, RestClientException {
        return this.schemaRegistry.getById(id);
    }

    public Schema getBySubjectAndId(String subject, int id) throws IOException, RestClientException {
        return this.schemaRegistry.getBySubjectAndId(subject, id);
    }
}
