//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package io.confluent.kafka.serializers;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaAvroSerializer extends AbstractKafkaAvroSerializer implements Serializer<Object> {
    private boolean isKey;

    public KafkaAvroSerializer() {
    }

    public KafkaAvroSerializer(SchemaRegistryClient client) {
        this.schemaRegistry = client;
    }

    public KafkaAvroSerializer(SchemaRegistryClient client, Map<String, ?> props) {
        this.schemaRegistry = client;
        this.configure(this.serializerConfig(props));
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        this.configure(new KafkaAvroSerializerConfig(configs));
    }

    public byte[] serialize(String topic, Object record) {
        return this.serializeImpl(this.getSubjectName(topic, this.isKey, record, AvroSchemaUtils.getSchema(record)), record);
    }

    public void close() {
    }
}
