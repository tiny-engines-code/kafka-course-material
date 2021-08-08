package com.chrislomeli.kafkautil.kafka;

import com.chrislomeli.kafkautil.ServiceConfiguration;
import com.chrislomeli.kafkautil.model.ClusterInfo;
import com.chrislomeli.kafkautil.model.SimpleTopicDescription;
import com.chrislomeli.kafkautil.model.TopicPartitionInfo;
import com.chrislomeli.kafkautil.model.TopicRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;
import org.springframework.http.HttpStatus;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class KafkaAdminService {

    AdminClient client;

    public KafkaAdminService() {
        ServiceConfiguration serviceConfiguration = ServiceConfiguration.getInstance();
        if (client==null) {
            client = KafkaAdminClient.create(serviceConfiguration.getAdminProperties());
        }
    }

    public ClusterInfo describeCluster() throws ExecutionException, InterruptedException {
        DescribeClusterResult cluster = client.describeCluster();
        String id = cluster.clusterId().get();
        Node nodes = cluster.controller().get();
        Set<AclOperation> acl = cluster.authorizedOperations().get();
        return ClusterInfo.builder()
                .id(id)
                .acl(acl)
                .hostInfo(nodes.toString())
                .build();
    }

    public Optional<SimpleTopicDescription> describeTopic(String topic) throws ExecutionException, InterruptedException, RestClientException {
        if (!topicExists(topic)) {
            throw new RestClientException(String.format("%s is not found", topic), HttpStatus.NOT_FOUND.value(), 404);
        }
        DescribeTopicsResult topics = client.describeTopics(Collections.singletonList(topic));
        Future<Map<String, TopicDescription>> topic_promise = topics.all();
        Map<String, TopicDescription> topicInfo = topic_promise.get();
        if (topicInfo == null || topicInfo.size() == 0)
            return Optional.empty();

        TopicDescription td = topicInfo.getOrDefault(topic, null);
        if (td == null)
            return Optional.empty();

        SimpleTopicDescription simpleTopicDescription = SimpleTopicDescription.builder()
                .topic(topic)
                .rawDescription(td.toString())
                .partitions(new ArrayList<>())
                .build();

        for (org.apache.kafka.common.TopicPartitionInfo tpi : td.partitions()) {
            simpleTopicDescription.getPartitions().add(
                    TopicPartitionInfo.builder()
                            .partition(tpi.partition())
                            .replicas(tpi.replicas().size())
                            .isrs(tpi.isr().size())
                            .leader(String.format("%s:%s", tpi.leader().host(), tpi.leader().port()))
                            .build());

        }
        return Optional.of(simpleTopicDescription);
    }


    public void saveTopic(TopicRequest topicInfo) throws ExecutionException, InterruptedException {
        NewTopic nt = new NewTopic(topicInfo.getTopic(), topicInfo.getPartitions(), topicInfo.getReplicas());
        List<NewTopic> ntList = Collections.singletonList(nt);
        log.info("Block while creating topic...");
        KafkaFuture<Void> response = client.createTopics(ntList).all();
        response.get();
    }


    public HttpStatus deleteTopic(String topic) throws RestClientException {
        log.info("Delete topic {}", topic);
        if (!topicExists(topic)) {
            throw new RestClientException(String.format("%s is not found", topic), HttpStatus.NOT_FOUND.value(), 404);
        }
        try {
            DeleteTopicsResult result = client.deleteTopics(Collections.singletonList(topic));
            result.all().get();
            return HttpStatus.OK;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error getting future while deleting {}: ", topic, e);
        }

        if (topicExists(topic)) {
            log.error("Topic still exists!: {}", topic);
            throw new RestClientException(String.format("%s still exists after delete", topic), HttpStatus.NOT_MODIFIED.value(), 404);
        } else {
            return HttpStatus.OK;
        }
    }


    public Optional<List<String>> listTopics() {
        log.info("List topics...");
        try {
            ListTopicsResult topics = client.listTopics();
            Set<String> topicNames = topics.names().get();
            Stream<String> topicSupplier = topicNames.stream().filter(s -> !s.startsWith("_"));
            return Optional.of(topicSupplier.collect(Collectors.toList()));

        } catch (Exception ex) {
            log.error("failed", ex);
            return Optional.empty();
        }
    }

    public boolean topicExists(String topicName) {
        try {
            ListTopicsResult topics = client.listTopics();
            Set<String> topicNames = topics.names().get();
            Supplier<Stream<String>> topicSupplier = () -> topicNames.stream().filter(s -> s.equals(topicName));
            Optional<Long> topicCount = Optional.of(topicSupplier.get().count());
            if (0 == topicCount.get()) {
                log.info("Search for topic {} returns none...", topicName);
                return false;
            }
            return true;

        } catch (Exception ex) {
            log.error("Exception while searchinf for topic {}:", topicName, ex);
            return false;
        }
    }
}
