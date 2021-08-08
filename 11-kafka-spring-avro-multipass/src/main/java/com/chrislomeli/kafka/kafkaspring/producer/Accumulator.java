package com.chrislomeli.kafka.kafkaspring.producer;

import java.util.concurrent.atomic.AtomicInteger;

public class Accumulator {
    static AtomicInteger sent = new AtomicInteger(0);
    static AtomicInteger completed = new AtomicInteger(0);
    static AtomicInteger success = new AtomicInteger(0);
    static AtomicInteger failed = new AtomicInteger(0);

}
