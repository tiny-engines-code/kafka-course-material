package com.chrislomeli.kafkautil.model;

import lombok.Builder;
import lombok.Data;

import java.util.Set;

@Builder
@Data
public class GenericRecordStats {
    Set<String> compatibleFields;
    Set<String> incompatibleFields;
    Set<String> unpopulatedSchemaFields;
    Set<String> unusedDataFields;
}
