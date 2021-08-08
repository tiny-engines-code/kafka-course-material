package com.nike.nsp.examples;

import com.nike.nsp.examples.config.ProducerConfigs;
import com.nike.nsp.examples.orderEventSchema.OrderEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerExample {

    @SuppressWarnings("unchecked")
    private static final Logger log = LogManager.getLogger(ProducerExample.class);

    // Return the current date and time as a String
    public static String getTimestamp() {
        return new Timestamp(System.currentTimeMillis()).toString();
    }

    public static void main(String[] args) {
        ProducerConfigs config = ProducerConfigs.fromEnv();
        Properties props = ProducerConfigs.createProperties(config);

        log.info("### PROPS: {} ", props);

        try {

            KafkaProducer producer = new KafkaProducer(props);
            // Create some OrderEvents to produce
            ArrayList<OrderEvent> orderEvents = new ArrayList<>();
            orderEvents.add(OrderEvent.newBuilder()
                    .setId(1)
                    .setTimestamp(getTimestamp())
                    .setProduct("Black Gloves")
                    .setPrice(12).build());
            orderEvents.add(OrderEvent.newBuilder()
                    .setId(2)
                    .setTimestamp(getTimestamp())
                    .setProduct("Black Hat")
                    .setPrice(30).build());
            orderEvents.add(OrderEvent.newBuilder()
                    .setId(3)
                    .setTimestamp(getTimestamp())
                    .setProduct("Gold Hat")
                    .setPrice(35).build());

            // Turn each OrderEvent into a ProducerRecord for the orders topic, and send them
            for (OrderEvent orderEvent : orderEvents) {
                ProducerRecord<String, OrderEvent> record = new ProducerRecord<>(config.getTopic(), orderEvent);
                Future<RecordMetadata> future = producer.send(record);
                RecordMetadata metadata = future.get();
                log.info("metadata - {}", metadata);
                log.info("sent - {}", record);
            }
            // Ensure all messages get sent to Kafka
            producer.flush();
            /*producer.metrics().forEach((key,value) -> {
                log.info(((KafkaMetric)value).metricName().name() + ": " + ((KafkaMetric)value).metricValue());
            });*/

        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

}