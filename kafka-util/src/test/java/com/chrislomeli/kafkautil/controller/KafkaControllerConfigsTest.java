package com.chrislomeli.kafkautil.controller;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;

@SpringBootTest
class KafkaControllerConfigsTest {

    @Autowired
    private KafkaServiceFacade service;

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void verifyWiring() {
        assertThat(service).isNotNull();
    }

    @Test
    void getBrokers() {
        assertThat(service.getBrokers()).isNotNull();
    }


    @Test
    void getSchemaRegistryUrl() {
        assertThat(service.getSchemaRegistryUrl()).isNotNull();
    }

//
//    @Test
//    void getTopics() {
//    }
//
//    @Test
//    void getSingleTopic() {
//    }
//
//    @Test
//    void getSchemas() {
//    }
//
//    @Test
//    void getSchemaByTopic() {
//    }
//
//    @Test
//    void saveSchemas() {
//    }
//
//    @Test
//    void deleteSchemas() {
//    }
//
//    @Test
//    void getTopicConsumer() {
//    }
//
//    @Test
//    void getTopicDataByOffset() {
//    }
//
//    @Test
//    void getTopicDataByDateTime() {
//    }
}