package com.chrislomeli.kafkautil.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopicPartitionInfo {
    int partition;
    int replicas;
    int isrs;
    long firsOffset = -1;
    long lastOffset = -1;
    String leader;
}
