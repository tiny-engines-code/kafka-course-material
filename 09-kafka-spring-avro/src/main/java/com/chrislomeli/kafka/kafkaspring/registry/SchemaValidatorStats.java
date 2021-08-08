package com.chrislomeli.kafka.kafkaspring.registry;

import lombok.Builder;
import lombok.Data;

import java.util.Set;

@Builder
@Data
public class SchemaValidatorStats {
    Set<String> compatibleFields;
    Set<String> incompatibleFields;
    Set<String> unpopulatedSchemaFields;
    Set<String> unusedDataFields;
}
