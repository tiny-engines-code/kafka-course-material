package com.chrislomeli.kafka.helloadmin.registry;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;

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
    Instant birthDate;
}

