package com.chrislomeli.kafka.kafkaspring.generator;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;
import java.util.List;

@Data
public class GenericRequests {
    List<GenericRequest> genericRequests;

    public GenericRequests(GenericRequest... genericRequests) {
        this.genericRequests = Arrays.asList(genericRequests);
    }
}
