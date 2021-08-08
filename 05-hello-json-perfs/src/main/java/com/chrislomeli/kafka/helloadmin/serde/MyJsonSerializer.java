package com.chrislomeli.kafka.helloadmin.serde;

import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

public class MyJsonSerializer<T> extends JsonSerializer<T> {

}
