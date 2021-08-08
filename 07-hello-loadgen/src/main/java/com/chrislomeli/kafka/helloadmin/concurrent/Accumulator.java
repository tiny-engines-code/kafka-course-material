package com.chrislomeli.kafka.helloadmin.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

public class Accumulator {
    public static AtomicInteger sent = new AtomicInteger(0);
    public static AtomicInteger delivered = new AtomicInteger(0);
    public static AtomicInteger responses = new AtomicInteger(0);
}
