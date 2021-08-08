package com.chrislomeli.kafka.helloadmin.concurrent;

import com.chrislomeli.kafka.helloadmin.generator.IDatumFactory;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableOnSubscribe;
import io.reactivex.annotations.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

import java.util.List;

@Slf4j
public class FlowableSubscriber implements FlowableOnSubscribe {
    IDatumFactory generator;
    int records;

    public FlowableSubscriber(IDatumFactory generator, int records) {
        this.records = records;
        this.generator = generator;
    }

    @Override
    public void subscribe(@NonNull FlowableEmitter flowableEmitter) {
        try {
            log.info("Entering the subscriber: while {} is less than {}...", Accumulator.sent.get(), records);

            while (Accumulator.sent.get() < records) {
                List<GenericRecord> nsl = generator.next();
                log.debug("\t....Do {} recs", nsl.size());
                for (GenericRecord gr : nsl) {
                    flowableEmitter.onNext(gr);
                    Accumulator.sent.incrementAndGet();
                }
                log.debug("\t....Current sent={} of {} recs", Accumulator.sent.get(), records);
            }
            log.info("Leaving the subscriber...");

        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }
    }

}