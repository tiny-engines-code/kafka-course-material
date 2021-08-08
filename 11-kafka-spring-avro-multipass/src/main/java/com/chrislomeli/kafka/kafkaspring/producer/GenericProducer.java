package com.chrislomeli.kafka.kafkaspring.producer;

import com.chrislomeli.kafka.kafkaspring.generator.GenericRequest;
import com.chrislomeli.kafka.kafkaspring.generator.GenericRequests;
import com.chrislomeli.kafka.kafkaspring.generator.NotificationStatusFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.time.Duration;
import java.time.Instant;

@Service
@Slf4j
public class GenericProducer {

  private final KafkaTemplate<String, GenericRecord> kafkaTemplate;

  @Value(value = "${notification_status_topic.name}")
  private String notificationStatusTopic;

  @Value(value = "${service_status_topic.name}")
  private String serviceStatusTopic;



  @Autowired
  public GenericProducer(KafkaTemplate<String, GenericRecord> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendMessages(int notifications) {

    NotificationStatusFactory generator = new NotificationStatusFactory(notificationStatusTopic, serviceStatusTopic);

    Instant start_time = Instant.now();
    int messageCount = 0;
    for (int i=0; i<notifications; i++) {
      for (GenericRequests gr : generator.next()) {
        for (GenericRequest genericRequest : gr.getGenericRequests()) {
          sendMessage(genericRequest);
          messageCount++;
        }
      }
      if (i % 5000 == 0)
        try {
          Instant end = Instant.now();
          long duration = Duration.between(start_time,end).toMillis();
          log.info("Sent {} records in {} mills == {} records per second", messageCount, duration, (((float)messageCount)/(duration/1000)));
          while (Accumulator.sent.get() > Accumulator.completed.get()) {
            log.debug("Waiting for {} records to catch up", Accumulator.sent.get() - Accumulator.completed.get());
          }
          end = Instant.now();
          duration = Duration.between(start_time,end).toMillis();
          log.info("Completed {} records in {} mills == {} effective records per second", messageCount, duration, (((float)messageCount)/(duration/1000)));
          Thread.sleep(100L);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
    }
    Instant end = Instant.now();
    long duration = Duration.between(start_time,end).toMillis();
    log.info("Completed {} records in {} mills == {} records per second", messageCount, duration, (((float)messageCount)/(duration/1000)));

  }

  public void sendMessage( GenericRequest genericRequest ) {
    Accumulator.sent.incrementAndGet();

    ListenableFuture<SendResult<String, GenericRecord>> future = this.kafkaTemplate.send(genericRequest.getTopic(),  genericRequest.getGenericRecord());
    future.addCallback(new ListenableFutureCallback<>() {
        @Override
        public void onSuccess(SendResult<String, GenericRecord> result) {
          RecordMetadata meta = result.getRecordMetadata();
          log.debug("Delivered {}<--[{}] at partition={}, offset={}", meta.topic(), genericRequest.getTopic(), meta.partition(), meta.offset());
          Accumulator.completed.incrementAndGet();
          Accumulator.success.incrementAndGet();
        }

        @Override
        public void onFailure(Throwable ex) {
          Accumulator.completed.incrementAndGet();
          Accumulator.failed.incrementAndGet();
          log.error(ex.getLocalizedMessage());
        }

    });
  }
}
