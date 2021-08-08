package com.chrislomeli.kafka.kafkaspring.registry;

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

    static Schema schema;

    public static void setSchema(String schemaString) {
        Schema.Parser parser = new Schema.Parser();
        GenericRecordFactory.schema = parser.parse(schemaString);
    }

    public static void setSchema(Schema schema) {
        GenericRecordFactory.schema = schema;
    }

    public static GenericRecord fromMap(Map<String, Object> fieldMap) {
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
        return fromMap(map);
    }

    private static Map<String, Object> pojoToMap(Object pojo) {
        // object
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
