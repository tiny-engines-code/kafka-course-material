package com.chrislomeli.kafka.helloadmin.generator;

import java.util.List;

public interface IDatumFactory<V> {
    public List<V> next();
}
