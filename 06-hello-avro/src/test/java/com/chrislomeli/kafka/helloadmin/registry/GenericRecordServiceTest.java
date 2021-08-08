package com.chrislomeli.kafka.helloadmin.registry;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GenericRecordServiceTest {

    @Test
    void validationTestValidPojo() {
        Object user = TestUser.builder()
                .userName("Jane Doe")
                .address("1000 Milky Way")
                .age(19)
                .id(20)
                .birthDate(Instant.now()).direction("NORTH")
                .experience(1)
                .build();

        Schema timestampMilliType = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        Schema sb = SchemaBuilder.record("Test").namespace("com.chrislomeli.kafka.common.schema")
                .fields()
                .requiredString("userName")
                .requiredString("address")
                .requiredInt("age")
                .name("birthDate").type(timestampMilliType).noDefault()
                .requiredInt("id")
                .requiredInt("experience")
                .requiredString("direction")
                .endRecord();

        String schemaString = sb.toString();
        SchemaValidatorStats stats = SchemaValidatorService.validateClass(sb, TestUser.class);

        assertNotNull(stats);
        assertNotNull(stats.getCompatibleFields());
        assertNotNull(stats.getIncompatibleFields());
        assertNotNull(stats.getUnpopulatedSchemaFields());
        assertNotNull(stats.getUnusedDataFields());
        assertEquals(7, stats.getCompatibleFields().size());
        assertEquals(0, stats.getIncompatibleFields().size());
        assertEquals(0, stats.getUnpopulatedSchemaFields().size());
        assertEquals(0, stats.getUnusedDataFields().size());
        assert (true);
    }

    @Test
    void validationTestMismatchedFields() {
        Object user = TestUser.builder()
                .userName("Jane Doe")
                .address("1000 Milky Way")
                .age(19)
                .id(20)
                .birthDate(Instant.now()).direction("NORTH")
                .experience(1)
                .build();

        Schema sb = SchemaBuilder.record("Test").namespace("com.chrislomeli.kafka.common.schema").fields()
                .requiredString("userName")
                .requiredString("address")
                .requiredString("country")
                .optionalBoolean("happy")
                .requiredInt("age")
                .endRecord();

        SchemaValidatorStats stats = SchemaValidatorService.validateClass(sb, TestUser.class);
        assertNotNull(stats);
        assertNotNull(stats.getCompatibleFields());
        assertNotNull(stats.getIncompatibleFields());
        assertNotNull(stats.getUnpopulatedSchemaFields());
        assertNotNull(stats.getUnusedDataFields());
        assertEquals(3, stats.getCompatibleFields().size());
        assertEquals(0, stats.getIncompatibleFields().size());
        assertEquals(2, stats.getUnpopulatedSchemaFields().size());
        assertEquals(4, stats.getUnusedDataFields().size());
        assert (true);
    }

    @Test
    void validationTestBadFields() {
        Object user = TestUser.builder()
                .userName("Jane Doe")
                .address("1000 Milky Way")
                .age(19)
                .id(20)
                .birthDate(Instant.now())
                .direction("NORTH")
                .experience(1)
                .build();

        Schema sb = SchemaBuilder.record("Test").namespace("com.chrislomeli.kafka.common.schema").fields()
                .requiredString("userName")
                .requiredString("address")
                .requiredInt("age")
                .requiredString("experience")
                .optionalString("id")
                .endRecord();

        SchemaValidatorStats stats = SchemaValidatorService.validateClass(sb, TestUser.class);
        assertNotNull(stats);
        assertNotNull(stats.getCompatibleFields());
        assertNotNull(stats.getIncompatibleFields());
        assertNotNull(stats.getUnpopulatedSchemaFields());
        assertNotNull(stats.getUnusedDataFields());
        assertEquals(3, stats.getCompatibleFields().size());
        assertEquals(2, stats.getIncompatibleFields().size());
        assertEquals(2, stats.getUnpopulatedSchemaFields().size());
        assertEquals(2, stats.getUnpopulatedSchemaFields().size());
        assert (true);
    }

    @Test
    void genericRecordFromMap() {

        TestUser user = TestUser.builder()
                .userName("John Doe")
                .address("1000 Nike Way")
                .age(36)
                .birthDate(Instant.now())
                .direction("NORTH")
                .experience(5)
                .build();

        Class<?> objClass = TestUser.class;
        Map<String, Object> fieldMap = new HashMap<>();
        java.lang.reflect.Field[] fields = objClass.getDeclaredFields();
        for (java.lang.reflect.Field fld : fields) {
            try {
                Object value = fld.get(user);
                fieldMap.put(fld.getName(), value);
            } catch (IllegalAccessException ignored) {

            }
        }

        Schema timestampMilliType = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        Schema sb = SchemaBuilder.record("Test").namespace("com.chrislomeli.kafka.common.schema")
                .fields()
                .requiredString("userName")
                .requiredString("address")
                .requiredInt("age")
                .name("birthDate").type(timestampMilliType).noDefault()
                .requiredInt("id")
                .requiredInt("experience")
                .requiredString("direction")
                .endRecord();

        GenericRecord r2 = new SchemaValidatorService(sb.toString()).genericRecordFromMap(fieldMap);
        assertNotNull(r2);
    }

    @Test
    void genericRecordFromJson() throws RestClientException, JsonProcessingException {
        String jsonString = "{" +
                "\"userName\": \"Jon Jason\"," +
                "\"address\": \"244 Microsoft Way\"," +
                "\"age\": 25," +
                "\"birthDate\": " + String.valueOf(Instant.now().toEpochMilli()) + "," +
                "\"experience\": 3," +
                "\"direction\": \"SOUTH\"" +
                "}";

        Schema timestampMilliType = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        Schema sb = SchemaBuilder.record("Test").namespace("com.chrislomeli.kafka.common.schema")
                .fields()
                .requiredString("userName")
                .requiredString("address")
                .requiredInt("age")
                .name("birthDate").type(timestampMilliType).noDefault()
                .requiredInt("id")
                .requiredInt("experience")
                .requiredString("direction")
                .endRecord();

        GenericRecord r2 = new SchemaValidatorService(sb.toString()).genericRecordFromJson(jsonString);
        assertNotNull(r2);
    }

}