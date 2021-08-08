package com.chrislomeli.kafka.kafkaspring.serde;

import org.springframework.kafka.support.serializer.JsonSerializer;

/*
  Just use the spring serializer, but for balance I'm including both the serializer and the deserializers in this package.
 */
public class MyJsonSerializer<T> extends JsonSerializer<T> {

}
