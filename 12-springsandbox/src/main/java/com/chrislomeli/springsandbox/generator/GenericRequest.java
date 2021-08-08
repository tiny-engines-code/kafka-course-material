package com.chrislomeli.springsandbox.generator;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.avro.generic.GenericRecord;

@Data
@AllArgsConstructor
public class GenericRequest {
    String topic;
    GenericRecord genericRecord;
}
