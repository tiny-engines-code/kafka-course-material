package com.chrislomeli.kafkautil.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TopicRequest {
    String topic;
    int partitions;
    short replicas;
}
