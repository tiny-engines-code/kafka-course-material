package com.chrislomeli.kafkautil.model;

import lombok.Builder;
import lombok.Data;
import org.apache.avro.generic.GenericRecord;

import java.util.Set;

@Builder
@Data
public class GenericRecordServiceResponse {
    GenericRecord genericRecord;
    Set<String> unpopulatedSchemaFields;
    Set<String> unusedDataFields;
}
