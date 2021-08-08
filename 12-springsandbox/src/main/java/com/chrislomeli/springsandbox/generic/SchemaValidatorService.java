package com.chrislomeli.springsandbox.generic;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/*
  Validate a pojo against a schema file.
  todo - not using this right now
 */
@Slf4j
public class SchemaValidatorService {

    @Builder
    @Data
    static public class SchemaValidatorStats {
        Set<String> compatibleFields;
        Set<String> incompatibleFields;
        Set<String> unpopulatedSchemaFields;
        Set<String> unusedDataFields;
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

        for (Field objField : fieldList) {
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
