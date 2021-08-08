package com.chrislomeli.kafka.kafkaspring.producer;

import com.chrislomeli.kafka.kafkaspring.generator.User;
import com.chrislomeli.kafka.kafkaspring.generator.UserFactory;
import lombok.extern.slf4j.Slf4j;
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

  private final KafkaTemplate<String, User> kafkaTemplate;

  @Autowired
  public GenericProducer(KafkaTemplate<String, User> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendMessages(int records) {
    UserFactory generator = new UserFactory();
    for (int i=0; i<records; i++) {
         sendMessage( generator.next() );
      }
  }

  public void sendMessage(User user) {

    ListenableFuture<SendResult<String, User>> future = this.kafkaTemplate.send(this.TOPIC,  user);

    future.addCallback(new ListenableFutureCallback<SendResult<String, User>>() {
      @Override
      public void onSuccess(SendResult<String, User> result) {
        RecordMetadata meta = result.getRecordMetadata();
        log.info("Delivered {}<--[{}] at partition={}, offset={}", meta.topic(), user.toString(), meta.partition(), meta.offset());
      }

      @Override
      public void onFailure(Throwable ex) {
        log.error(ex.getLocalizedMessage());
      }
    });
  }
}
