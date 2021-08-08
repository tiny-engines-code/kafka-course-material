package com.chrislomeli.kafkautil.kafka;

import com.chrislomeli.kafkautil.model.TopicPartitionStats;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.DateTime;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class KafkaConsumerService<K, V> implements AutoCloseable {

    private static ObjectMapper mapper = new ObjectMapper();
    Properties properties;
    KafkaConsumer<K, V> consumer;

    public KafkaConsumerService(Properties properties) {
        log.info("Enter KafkaProducerService");
        if (consumer == null)
            consumer = new KafkaConsumer<K, V>(properties);
        log.info("instantiated KafkaProducerService...");
    }

    public void close() {
        if (consumer != null)
            try { consumer.close(); } catch (Exception ignored) {}
    }

    public Collection<TopicPartitionStats> getPartitionInfo(String topic) {
        List<PartitionInfo> patritions = consumer.partitionsFor(topic);
        List<TopicPartition> topicPartitions = consumer.partitionsFor(topic).stream()
                .map(x -> new TopicPartition(x.topic(), x.partition()))
                .collect(Collectors.toList());

        Map<Integer, TopicPartitionStats> partitionInfo = new HashMap<>();
        for (TopicPartition topicPartition : topicPartitions) {
            consumer.assign(Collections.singletonList(topicPartition));
            consumer.poll(Duration.ofMillis(0));
            consumer.commitSync();
            consumer.seekToEnd(Collections.singletonList(topicPartition));

            TopicPartitionStats stats = TopicPartitionStats.builder()
                    .topic(topicPartition.topic())
                    .lastOffset(consumer.position(topicPartition) - 1)
                    .partition(topicPartition.partition())
                    .build();

            consumer.seekToBeginning(Collections.singletonList(topicPartition));
            stats.setFirstOffset(consumer.position(topicPartition));
            partitionInfo.put(topicPartition.partition(), stats);
        }
        return partitionInfo.values();
    }

    public List<Map<String, Object>> getPageByOffset(String topic, int partition, long offset) {
        List<TopicPartition> topicPartitions = consumer.partitionsFor(topic).stream()
                .filter(x -> x.partition() == partition)
                .map(x -> new TopicPartition(x.topic(), x.partition()))
                .collect(Collectors.toList());

        List<Map<String, Object>> cheese = new ArrayList<>();
        if (topicPartitions.isEmpty()) {
            return null;
        }

        TopicPartition topicPartition = topicPartitions.get(0);
        consumer.assign(Collections.singletonList(topicPartition));

        consumer.seekToEnd(Collections.singletonList(topicPartition));
        long nextOffset = consumer.position(topicPartition);

        return readAtOffset(topicPartition, offset);

    }

    public List<Map<String, Object>> getPageByTimestamp(String topicName, long timeStamp) {

        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topicName);
        List<TopicPartition> topicPartitionList = partitionInfos
                .stream()
                .map(info -> new TopicPartition(topicName, info.partition()))
                .collect(Collectors.toList());

        consumer.assign(topicPartitionList);

        Map<TopicPartition, Long> partitionTimestampMap = topicPartitionList.stream()
                .collect(Collectors.toMap(tp -> tp, tp -> timeStamp));

        Map<TopicPartition, OffsetAndTimestamp> partitionOffsetMap = consumer.offsetsForTimes(partitionTimestampMap);

        List<Map<String, Object>> cheese = new ArrayList<>();

        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : partitionOffsetMap.entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetAndTimestamp offsetAndTimestamp = entry.getValue();
            if (tp == null || offsetAndTimestamp == null) continue;
            List<Map<String, Object>> some = readAtOffset(tp, offsetAndTimestamp.offset());
            cheese.addAll(some);
        }

        return cheese;
    }


    public List<Map<String, Object>> readAtOffset(TopicPartition topicPartition, long currentOffset) {

        consumer.seek(topicPartition, currentOffset);
        consumer.commitSync();

        DateTime endTime = DateTime.now().plusSeconds(4);
        List<Map<String, Object>> recordCollector = new ArrayList<>();

        while (recordCollector.size() == 0 && endTime.isAfter(DateTime.now())) {
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<K, V> record : records) {
                currentOffset = record.offset();
                String json = String.valueOf(record.value());
                Map<String, Object> map = new HashMap<>();
                map.put("Partition", record.partition());
                map.put("Offset", currentOffset);
                map.put("Timestamp", record.timestamp());
                map.put("Key", record.key());
                map.put("Headers", record.headers());
                map.put("Raw", json);
                try {
                    Map<String, String> recordMap = mapper.readValue(json.getBytes(), Map.class);
                    map.put("Data", recordMap);
                    recordCollector.add(map);
                } catch (IOException ignored) {
                    recordCollector.add(map);
                    ;
                }
            }
        }

        if (recordCollector.size() == 0) {
            log.error("Timed out waiting for kafka to retreive rows...");
        }
        return recordCollector;
    }

}
