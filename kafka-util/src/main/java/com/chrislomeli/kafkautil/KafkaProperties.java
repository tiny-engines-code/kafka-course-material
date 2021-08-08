package com.chrislomeli.kafkautil;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@PropertySource("classpath:application.yml")
@ConfigurationProperties("kafka")
@Data
public class KafkaProperties {
    private String registry_url;
    private String application_id;
    private int page_size;
    private List<String> servers;
}