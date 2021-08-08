package com.chrislomeli.kafkautil.kafka.core;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.joda.time.DateTime;

@Builder
@Data
@AllArgsConstructor
public class TestUser {
    String userName;
    String address;
    int age;
    int id;
    int experience;
    String direction;
    DateTime birthDate;
}

