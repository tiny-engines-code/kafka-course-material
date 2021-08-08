package com.chrislomeli.kafka.helloadmin.generator;

import com.github.javafaker.Faker;

import java.time.Instant;
import java.util.Random;

public class UserFactory<V> {

    static final Faker faker = new Faker();
    static final Random randomize = new Random();

    public V nextUser() {
        int age = randomize.nextInt(80);
        Instant birthTime = Instant.now().minusSeconds(86400 * 365 * age);

        return (V) User.builder()
                .userName(faker.name().username())
                .emailAddress(faker.internet().emailAddress())
                .age(age)
                .id((long) randomize.nextInt(1000000))
                .birthTime(birthTime)
                .build();
    }


}
