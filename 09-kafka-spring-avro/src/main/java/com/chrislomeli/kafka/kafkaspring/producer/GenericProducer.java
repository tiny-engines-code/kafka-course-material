package com.chrislomeli.kafka.kafkaspring.producer;

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

@Service
@Slf4j
public class GenericProducer {

  @Value("${topic.name}")
  private String TOPIC;

  private final KafkaTemplate<String, GenericRecord> kafkaTemplate;

  @Autowired
  public GenericProducer(KafkaTemplate<String, GenericRecord> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendMessages(int records) {

    NotificationStatusFactory generator = new NotificationStatusFactory();

    for (int i=0; i<records;) {
      for (GenericRecord gr : generator.next()) {
        i++;
        sendMessage(gr);
      }
    }
  }

  public void sendMessage(GenericRecord notificationStatus) {

    ListenableFuture<SendResult<String, GenericRecord>> future = this.kafkaTemplate.send(this.TOPIC,  notificationStatus);

    future.addCallback(new ListenableFutureCallback<SendResult<String, GenericRecord>>() {
      @Override
      public void onSuccess(SendResult<String, GenericRecord> result) {
        RecordMetadata meta = result.getRecordMetadata();
        log.info("Delivered {}<--[{}] at partition={}, offset={}", meta.topic(), notificationStatus.toString(), meta.partition(), meta.offset());
      }

      @Override
      public void onFailure(Throwable ex) {
        log.error(ex.getLocalizedMessage());
      }
    });
  }
}
