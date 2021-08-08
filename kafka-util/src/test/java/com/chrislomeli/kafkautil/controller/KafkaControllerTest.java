package com.chrislomeli.kafkautil.controller;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;

@WebMvcTest(KafkaUtilController.class)
class KafkaControllerTest {

    @Autowired
    private MockMvc restController;

    @MockBean
    private KafkaServiceFacade service;

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void verifyWiring() {
        assertThat(restController).isNotNull();
    }

    @Test
    void ping() throws Exception {
        when(service.ping()).thenReturn("pong");
        this.restController.perform(
                get("/ping"))
                .andDo(print()).andExpect(status().isOk())
                .andExpect(content().string(containsString("pong")));
    }

    @Test
    void getBrokers() {

    }

    //
//    @Test
//    void setBrokers() {
//    }
//
//    @Test
//    void getSchemaRegistryUrl() {
//    }
//
//    @Test
//    void setSchemaRegistryUrl() {
//    }
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