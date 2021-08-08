package com.chrislomeli.kafkautil.kafka.core;


import com.chrislomeli.kafkautil.kafka.GenericRecordService;
import com.chrislomeli.kafkautil.model.GenericRecordServiceResponse;
import com.chrislomeli.kafkautil.model.GenericRecordStats;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class GenericRecordServiceTest {

    public static String getSchemaString() {
        Schema sb = SchemaBuilder.record("Test").namespace("com.chrislomeli.kafka.common.schema").fields()
                .requiredString("UserName")
                .requiredString("address")
                .requiredInt("age")
                .requiredLong("birthDate")
                .name("direction").type()
                    .enumeration("direction").namespace("org.apache.test")
                    .aliases("org.apache.test.OldSuit")
                    .doc("CardSuits")
                    .prop("customProp", "val")
                    .symbols("NORTH", "SOUTH", "EAST", "WEST")
                    .enumDefault("NORTH")
                .endRecord();

        String unionExample = " { \"name\" : \"experience\", \"type\": [\"int\", \"null\"] },";
        String enumStringExample =
                "        {\n" +
                        "            \"name\": \"direction\",\n" +
                        "            \"type\": {\n" +
                        "                \"type\": \"enum\",\n" +
                        "                \"name\": \"Compass\",\n" +
                        "                \"namespace\": \"co.uk.dalelane\",\n" +
                        "                \"symbols\": [\n" +
                        "                    \"NORTH\", \"SOUTH\", \"EAST\", \"WEST\"\n" +
                        "                ]\n" +
                        "            }\n" +
                        "        },";

        return "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"namespace\": \"com.chrislomeli.kafka.common.schema\",\n" +
                "  \"name\": \"MySchema2\",\n" +
                "  \"fields\": [\n" +
                unionExample + enumStringExample +
                "    {\n" +
                "      \"name\": \"userName\",\n" +
                "      \"type\": \"string\",\n" +
                "      \"avro.java.string\": \"String\",\n" +
                "      \"doc\": \"example user name\"\n" +
                "    },\n" +
                "     {\n" +
                "      \"name\": \"address\",\n" +
                "      \"type\": \"string\",\n" +
                "      \"avro.java.string\": \"String\",\n" +
                "      \"doc\": \"example user address\"\n" +
                "    },\n" +
                "     {\n" +
                "      \"name\": \"age\",\n" +
                "      \"type\": \"int\",\n" +
                "      \"doc\": \"example user age\"\n" +
                "    },   {\n" +
                "      \"name\": \"birthDate\",\n" +
                "      \"type\": {\n" +
                "        \"type\": \"long\",\n" +
                "        \"logicalType\": \"timestamp-millis\"\n" +
                "      },\n" +
                "      \"doc\": \"Date and time of birth\"\n" +
                "    }\n" +
                "\n" +
                "  ]\n" +
                "}";

    }

    @Test
    void validationTestExtraPojoFields() {
        Object user = TestUser.builder()
                .userName("Jane Doe")
                .address("1000 Milky Way")
                .age(19)
                .id(20)
                .birthDate(DateTime.now()).direction("NORTH")
                .experience(1)
                .build();

        Schema sb = SchemaBuilder.record("Test").namespace("com.chrislomeli.kafka.common.schema").fields()
                .requiredString("userName")
                .requiredString("address")
                .requiredInt("age")
                .endRecord();

        GenericRecordService service = new GenericRecordService(sb.toString());
        GenericRecordStats stats = GenericRecordService.validateClass(sb, TestUser.class);
        assertNotNull(stats);
        assertNotNull(stats.getCompatibleFields());
        assertNotNull(stats.getIncompatibleFields());
        assertNotNull(stats.getUnpopulatedSchemaFields());
        assertNotNull(stats.getUnusedDataFields());
        assertEquals(3, stats.getCompatibleFields().size());
        assertEquals(0, stats.getIncompatibleFields().size());
        assertEquals(0, stats.getUnpopulatedSchemaFields().size());
        assertEquals(4, stats.getUnusedDataFields().size());
        assert(true);
    }

    @Test
    void validationTestExtraSchemaFields() {
        Object user = TestUser.builder()
                .userName("Jane Doe")
                .address("1000 Milky Way")
                .age(19)
                .id(20)
                .birthDate(DateTime.now()).direction("NORTH")
                .experience(1)
                .build();

        Schema sb = SchemaBuilder.record("Test").namespace("com.chrislomeli.kafka.common.schema").fields()
                .requiredString("userName")
                .requiredString("address")
                .requiredString("country")
                .optionalBoolean("happy")
                .requiredInt("age")
                .endRecord();

        GenericRecordService service = new GenericRecordService(sb.toString());
        GenericRecordStats stats = GenericRecordService.validateClass(sb, TestUser.class);
        assertNotNull(stats);
        assertNotNull(stats.getCompatibleFields());
        assertNotNull(stats.getIncompatibleFields());
        assertNotNull(stats.getUnpopulatedSchemaFields());
        assertNotNull(stats.getUnusedDataFields());
        assertEquals(3, stats.getCompatibleFields().size());
        assertEquals(0, stats.getIncompatibleFields().size());
        assertEquals(2, stats.getUnpopulatedSchemaFields().size());
        assertEquals(4, stats.getUnusedDataFields().size());
        assert(true);
    }

    @Test
    void validationTestBadFields() {
        Object user = TestUser.builder()
                .userName("Jane Doe")
                .address("1000 Milky Way")
                .age(19)
                .id(20)
                .birthDate(DateTime.now())
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

        GenericRecordService service = new GenericRecordService(sb.toString());
        GenericRecordStats stats = GenericRecordService.validateClass(sb, TestUser.class);
        assertNotNull(stats);
        assertNotNull(stats.getCompatibleFields());
        assertNotNull(stats.getIncompatibleFields());
        assertNotNull(stats.getUnpopulatedSchemaFields());
        assertNotNull(stats.getUnusedDataFields());
        assertEquals(3, stats.getCompatibleFields().size());
        assertEquals(2, stats.getIncompatibleFields().size());
        assertEquals(2, stats.getUnpopulatedSchemaFields().size());
        assert(true);
    }

    @Test
    void genericRecordFromPojo() {
        TestUser user = TestUser.builder()
                .userName("Jane Doe")
                .address("1000 Milky Way")
                .age(19)
                .birthDate(DateTime.now()).direction("NORTH")
                .experience(1)
                .build();

        GenericRecordServiceResponse r = new GenericRecordService(getSchemaString()).genericRecordFromPojo(user);
        assertNotNull(r);
    }

    @Test
    void genericRecordFromMap() {

        TestUser user = TestUser.builder()
                .userName("John Doe")
                .address("1000 Nike Way")
                .age(36)
                .birthDate(DateTime.now()).direction("NORTH")
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

        GenericRecordServiceResponse r2 = new GenericRecordService(getSchemaString()).genericRecordFromMap(fieldMap);
        assertNotNull(r2);
    }

    @Test
    void genericRecordFromJson() throws RestClientException, JsonProcessingException {
        String jsonString = "{" +
                "\"userName\": \"Jon Jason\"," +
                "\"address\": \"244 Microsoft Way\"," +
                "\"age\": 25," +
                "\"birthDate\": " + String.valueOf(DateTime.now().getMillis()) + "," +
                "\"experience\": 3," +
                "\"direction\": \"SOUTH\"" +
                "}";

        GenericRecordServiceResponse r2 = new GenericRecordService(getSchemaString()).genericRecordFromJson(jsonString);
        assertNotNull(r2);
    }
}