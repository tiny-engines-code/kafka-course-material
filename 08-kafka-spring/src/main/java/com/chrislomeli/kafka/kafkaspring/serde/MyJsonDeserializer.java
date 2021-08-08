package com.chrislomeli.kafka.kafkaspring.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/* The springboot Json deserializer requires registering the class of the object
 *  as a trusted class.
 * Since this is just an example I'm just using a home-grown version based on an internet-available hack.
 */
@NoArgsConstructor
public class MyJsonDeserializer<T> implements Deserializer<T> {
    private ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    ;
    private Class<T> clazzName;
    public static final String KEY_CLASS_NAME_CONFIG = "key.class.name";
    public static final String VALUE_CLASS_NAME_CONFIG = "value.class.name";

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        if (isKey)
            clazzName = (Class<T>) props.get(KEY_CLASS_NAME_CONFIG);
        else
            clazzName = (Class<T>) props.get(VALUE_CLASS_NAME_CONFIG);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, clazzName);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {
        //nothing to close
    }
}
