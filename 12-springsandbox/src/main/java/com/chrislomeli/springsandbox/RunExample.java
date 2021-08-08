package com.chrislomeli.springsandbox;

import com.chrislomeli.springsandbox.generator.GenericRequest;
import com.chrislomeli.springsandbox.generator.NotificationStatusFactory;
import com.chrislomeli.springsandbox.producer.ProducerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Component
@Slf4j
public class RunExample implements CommandLineRunner {

    final ProducerService svc;

    @Value(value = "${notification_status_topic.name}")
    private String notificationStatusTopic;

    @Value(value = "${service_status_topic.name}")
    private String serviceStatusTopic;

    @Value(value = "${app.with-overhead}")
    private String overhead;

    @Value(value = "${app.runtime-ms}")
    private String runtime;

    @Value(value = "${app.messages-per-second}")
    private String messagesPerSecondStr;

    public RunExample(ProducerService svc) {
        this.svc = svc;
    }

    public void runTrial() throws Exception {

        int runTimeMs = Integer.parseInt(runtime);
        float targetRps = Integer.parseInt(messagesPerSecondStr);
        int totalSendCount = 0;
        NotificationStatusFactory generator = new NotificationStatusFactory(notificationStatusTopic, serviceStatusTopic);

        boolean generateNew = "true".equals(overhead);
        List<GenericRequest> requestList = null;

        log.info("\n------------------\nProduce records Async for {} millis - throttle to {} TPS\n-----------------", runTimeMs, targetRps);
        // loop and generate payloads for the duration configured
        Instant start = Instant.now();
        do {
            if (totalSendCount % 10000 == 0 ) {
                log.info("Created {} recs", totalSendCount);
            }

            // generateNew means that we generate a new payload for every call - and that generating time
            //   affects our TPS and is not pure Kafka - it's our own test setup time which could be shorter or longer
            //   than the real time to create notification status
            if ( generateNew || requestList == null )
                requestList = generator.next();

            for (GenericRequest genericRequest : requestList) {
                svc.sendProducerRecord(genericRequest.getTopic(), genericRequest.getGenericRecord());
                totalSendCount++;
            }

            // every thousand messages compare our current rate against our targetRps and throttle
            if (totalSendCount % 1000 == 0 && targetRps > 0)
                    throttleMessages(totalSendCount, start);

        } while (Duration.between(start, Instant.now()).toMillis() < runTimeMs);

        long millis = Duration.between(start, Instant.now()).toMillis();
        log.info("\n-----Job Complete: Completed {} records in milliseconds={}- rps={}---------", totalSendCount, millis, totalSendCount/(millis/1000));

        System.exit(0);
    }

    // throttle the generator so that we stay close to the target TPS
    private void throttleMessages(int totalSendCount, Instant start ) {
        int runTimeMs = Integer.parseInt(runtime);
        float targetRps = Integer.parseInt(messagesPerSecondStr);
        if (totalSendCount % 1000 == 0 ) {
            while ( Duration.between(start, Instant.now()).toMillis() < runTimeMs  )
            {
                long currentSecs =  (Duration.between(start, Instant.now()).toSeconds());
                if (currentSecs == 0)
                    break;
                float currentRps = (float)totalSendCount/currentSecs;
                double targetSeconds = Math.floor((float)totalSendCount/targetRps);
                double waitMillis = (targetSeconds - currentSecs) * 1000;
                if (waitMillis > 10) {
                    waitMillis = (targetSeconds - currentSecs) * 1000;
                    try {Thread.sleep((long) waitMillis); } catch (Exception ignored){}
                } else {
                    break;
                }
            }
        }

    }

    @Override
    public void run(String... args) throws Exception {
        runTrial();
        System.exit(0);
    }


}
