package com.chrislomeli.kafka.helloadmin.concurrent;

import com.chrislomeli.kafka.helloadmin.generator.IDatumFactory;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

@Slf4j
public class FlowableController {

    private Disposable subscription;
    private FlowableWorker worker;
    int records;
    IDatumFactory generator;

    public FlowableController(String topicName, KafkaProducer<String, GenericRecord> producer, IDatumFactory generator, int records) {
        this.worker = new FlowableWorker(topicName, producer);
        this.records = records;
        this.generator = generator;
    }

    public void stop() {
        subscription.dispose();
    }

    public void start() {
        log.info("Enter the RunLoop...");

        FlowableSubscriber subscriber = new FlowableSubscriber(generator, records);

        Flowable<GenericRecord> publisher = Flowable.create(subscriber, BackpressureStrategy.BUFFER);

        log.info("Run the publisher");
        subscription = publisher
                .subscribe((x) -> worker.onNext(x));

        log.info("Leaving the publisher with stats sent={}, delivered={}, responses={}...", Accumulator.sent.get(), Accumulator.delivered.get(), Accumulator.responses.get());

        while (Accumulator.responses.get() < Accumulator.delivered.get()) {
            log.info("Waiting for all callbacks to respond ...");
            try {
                Thread.sleep(1000);
            } catch (Exception ignored) {
            }
        }

        log.info("Leaving the publisher...");
    }
}
