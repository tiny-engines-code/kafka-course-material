package com.chrislomeli.kafkautil.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicPartitionStats {
    String topic;
    int partition;
    long firstOffset;
    long lastOffset;
}
