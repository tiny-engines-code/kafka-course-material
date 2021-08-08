package com.chrislomeli.kafka.helloadmin.registry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class SchemaValidatorService {

    static ObjectMapper mapper = new ObjectMapper();

    Schema schema;

    public SchemaValidatorService(String schemaString) {
        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(schemaString);
    }

    public SchemaValidatorService(Schema schema) {
        this.schema = schema;
    }

    public GenericRecord genericRecordFromMap(Map<String, Object> fieldMap) {
        GenericRecord record = new GenericData.Record(schema);

        for (Schema.Field field : record.getSchema().getFields()) {
            String fieldKey = field.name();
            Object value = fieldMap.getOrDefault(fieldKey, null);
            if (value != null) {
                record.put(fieldKey, value);
                fieldMap.remove(fieldKey);
            }
            log.info(fieldKey + " : " + record.get(fieldKey));
        }

        return record;
    }

    public GenericRecord genericRecordFromPojo(Object pojo) {
        Map<String, Object> map = pojoToMap(pojo);
        return genericRecordFromMap(map);
    }

    public GenericRecord genericRecordFromJson(String jsonString) throws JsonProcessingException {
        Map<String, Object> map = mapper.readValue(jsonString, new TypeReference<>() {
        });
        return genericRecordFromMap(map);
    }

    public Map<String, Object> pojoToMap(Object pojo) {
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


    /**
     * static utils
     **/
    public static boolean compatibleFieldTypes(Schema.Field field, Class<?> clazz) {
        Schema.Type type = field.schema().getType();

        Optional<Map<String, Object>> props = Optional.empty();

        Schema fieldSchema = field.schema();
        LogicalType logicalType = fieldSchema.getLogicalType();

        // handle simple null, value unions
        boolean nullable = false;
        if (type == Schema.Type.UNION) {
            List<Schema> types = field.schema().getTypes();
            log.info("types");
            nullable = types.stream().anyMatch(x -> x.getType() == Schema.Type.NULL);
            List<Schema> realTypes = types.stream().filter(x -> x.getType() != Schema.Type.NULL).collect(Collectors.toList());
            if (realTypes.size() > 0)
                type = realTypes.get(0).getType();
        }

        if (Instant.class.equals(clazz) && type == Schema.Type.LONG && logicalType.getName().startsWith("timestamp-millis"))
            return false;
        if (String.class.equals(clazz) && type == Schema.Type.STRING) return false;
        if (Long.class.equals(clazz) || long.class.equals(clazz) && type == Schema.Type.LONG) return false;
        if (Integer.class.equals(clazz) || int.class.equals(clazz) && type == Schema.Type.INT) return false;
        if (Boolean.class.equals(clazz) || boolean.class.equals(clazz) && type == Schema.Type.BOOLEAN) return false;
        if (Byte.class.equals(clazz) || byte.class.equals(clazz) && type == Schema.Type.BYTES) return false;
        if (Float.class.equals(clazz) || float.class.equals(clazz) && type == Schema.Type.FLOAT) return false;
        if (Double.class.equals(clazz) || double.class.equals(clazz) && type == Schema.Type.DOUBLE) return false;

        switch (type) {
            case ENUM:
                List<?> symbols = field.schema().getEnumSymbols();
                if (symbols.size() == 0)
                    throw new RuntimeException(String.format("Enum %s is empty ", field.name()));
                Object example = symbols.get(0);
                if (clazz.isInstance(example)) {
                    return false;
                }

                // checked
            case STRING:
            case LONG:
            case INT:
            case BOOLEAN:
            case BYTES:
            case FLOAT:
            case DOUBLE:
                log.error("{} type was not expected for {}", type.getName(), field.name());
                return true;

            // unchecked
            case NULL:
            case MAP:
            case RECORD:
            case UNION:
            case ARRAY:
            case FIXED:
            default:
                throw new RuntimeException(String.format("Type %s is not checked", type.getName()));
        }
    }

    public static SchemaValidatorStats validateClass(Schema schema, Class<?> classRef) {
        Field[] fieldList = classRef.getDeclaredFields();

        Map<String, Schema.Field> schemaFields = new HashMap<>();
        schema.getFields().forEach(x -> schemaFields.put(x.name(), x));

        Set<String> missingInSchema = new HashSet<>();
        Set<String> incompatibleFields = new HashSet<>();
        Set<String> compatibleFields = new HashSet<>();

        for (java.lang.reflect.Field objField : fieldList) {
            objField.setAccessible(true);
            Class<?> pojoFieldType = objField.getType();
            String pojoFieldName = objField.getName();
            Optional<Schema.Field> schemaField = Optional.ofNullable(schemaFields.getOrDefault(pojoFieldName, null));
            if (schemaField.isEmpty()) {
                missingInSchema.add(objField.getName());
            } else if (compatibleFieldTypes(schemaField.get(), pojoFieldType)) {
                incompatibleFields.add(pojoFieldName);
            } else {
                schemaFields.remove(pojoFieldName);
                compatibleFields.add(pojoFieldName);
            }
        }

        return SchemaValidatorStats.builder()
                .incompatibleFields(incompatibleFields)
                .unpopulatedSchemaFields(schemaFields.keySet())
                .unusedDataFields(missingInSchema)
                .compatibleFields(compatibleFields)
                .build();
    }

}
