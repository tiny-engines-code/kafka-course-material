package com.chrislomeli.kafka.helloadmin.generator;

import com.github.javafaker.Faker;

import java.time.Instant;
import java.util.Random;

public class UserFactory {

    static final Faker faker = new Faker();
    static final Random randomize = new Random();

    public User nextUser() {
        int age = randomize.nextInt(80);
        Instant birthTime = Instant.now().minusSeconds(86400 * 365 * age);

        return User.builder()
                .userName(faker.name().username())
                .emailAddress(faker.internet().emailAddress())
                .age(age)
                .id((long) randomize.nextInt(1000000))
                .birthTime(birthTime)
                .build();
    }


}
