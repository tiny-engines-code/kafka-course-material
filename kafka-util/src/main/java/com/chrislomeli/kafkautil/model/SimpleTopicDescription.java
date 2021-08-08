package com.chrislomeli.kafkautil.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.List;

@SuperBuilder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SimpleTopicDescription {
    String topic;
    List<TopicPartitionInfo> partitions;
    String rawDescription;
    boolean hasSchema;
}
