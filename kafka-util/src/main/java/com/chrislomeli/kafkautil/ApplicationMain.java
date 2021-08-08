package com.chrislomeli.kafkautil;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class ApplicationMain {

    public static void main(String[] args) {
        if (args.length > 0)
            ServiceConfiguration.envPropertiesFile = args[0];
        SpringApplication.run(ApplicationMain.class, args);
    }
}
