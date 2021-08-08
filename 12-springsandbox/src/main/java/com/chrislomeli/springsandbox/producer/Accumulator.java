package com.chrislomeli.springsandbox.producer;

import java.util.concurrent.atomic.AtomicInteger;

// just an example - todo: used as an example in the producer but not essential
public class Accumulator {
    static AtomicInteger sent = new AtomicInteger(0);
    static AtomicInteger completed = new AtomicInteger(0);
    static AtomicInteger success = new AtomicInteger(0);
    static AtomicInteger failed = new AtomicInteger(0);

}
