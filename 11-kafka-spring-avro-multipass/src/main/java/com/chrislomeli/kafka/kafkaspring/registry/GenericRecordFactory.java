package com.chrislomeli.kafka.kafkaspring.registry;

import com.chrislomeli.kafka.kafkaspring.generator.NotificationStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class GenericRecordFactory {

    @AllArgsConstructor
    @Builder
    @Data
    static class TopicAttributes {
        String topic;
        Schema schema;
        Class<?> clazz;
    }

    static Map<String, TopicAttributes> schemas;

    public static void setSchema(String topic, Class<?> clazz, String schemaString) {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);
        setSchema(topic, clazz, schema);
    }

    public static void setSchema(String topic, Class<?> clazz, Schema schema) {
        if (schemas == null)
            schemas = new HashMap<>();
        schemas.put(clazz.getName(), TopicAttributes.builder()
                .schema(schema)
                .clazz(clazz)
                .topic(topic)
                .build());

    }

    public static GenericRecord fromMap(Map<String, Object> fieldMap, Schema schema) {
        GenericRecord record = new GenericData.Record(schema);

        for (Schema.Field field : record.getSchema().getFields()) {
            String fieldKey = field.name();
            Schema fieldSchema = field.schema();
            Object value = fieldMap.getOrDefault(fieldKey, null);
            if (value != null) {
                record.put(fieldKey, performConversions(fieldSchema, value));
                fieldMap.remove(fieldKey);
            }
        }

        return record;
    }

    private static Object performConversions(Schema fieldSchema, Object value) {
        LogicalType logicalType = fieldSchema.getLogicalType();

        if (value.getClass().equals(Instant.class)) {
            return ((Instant) value).toEpochMilli();
        }
        return value;
    }

    public static GenericRecord fromPojo(Object pojo) {
        Map<String, Object> map = pojoToMap(pojo);

        Class<?> objClass = pojo.getClass();
        String className = objClass.getName();
        TopicAttributes topicAttributes =  schemas.get(className);
        if (topicAttributes == null) throw new RuntimeException(String.format("Can't find a mapping for class %s", className));

        return fromMap(map, topicAttributes.getSchema());
    }

    private static Map<String, Object> pojoToMap(Object pojo) {
        Class<?> objClass = pojo.getClass();
        Map<String, Object> fieldMap = new HashMap<>();
        Field[] fieldList = objClass.getDeclaredFields();

        for (Field objField : fieldList) {
            try {
                objField.setAccessible(true);
                Object value = objField.get(pojo);
                fieldMap.put(objField.getName(), value);
            } catch (IllegalAccessException e) {
                log.error("Exception while populating GenericRecord for pojo class {}", objClass.getName());
            }
        }
        return fieldMap;
    }

}
