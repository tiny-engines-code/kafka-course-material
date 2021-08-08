package com.chrislomeli.kafka.helloadmin.admin;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class KafkaAdminUtils implements AutoCloseable {

    static AdminClient client = null;

    public KafkaAdminUtils(List<String> bootstrapServersList) {
        String bootstrapServers = StringUtils.collectionToCommaDelimitedString(bootstrapServersList);
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        client = AdminClient.create(config);
    }

    /* Topic Utilities */
    public boolean topicNotExists(String topic) throws ExecutionException, InterruptedException {
        Optional<List<String>> topicsList = getTopics();
        if (topicsList.isEmpty())
            return true;
        long topicFound = topicsList.get().stream().filter(s -> s.equals(topic)).count();
        return topicFound <= 0;
    }

    public Optional<List<String>> getTopics() throws ExecutionException, InterruptedException {
        ListTopicsResult topics = client.listTopics();
        Set<String> topicNames = topics.names().get();
        Stream<String> topicSupplier = topicNames.stream().filter(s -> !s.startsWith("_"));
        return Optional.of(topicSupplier.collect(Collectors.toList()));
    }

    public void saveTopic(String topic, int partitions, short replicas) throws ExecutionException, InterruptedException {
        NewTopic nt = new NewTopic(topic, partitions, replicas);
        List<NewTopic> ntList = Collections.singletonList(nt);
        KafkaFuture<Void> response = client.createTopics(ntList).all();  //block
        response.get();
    }

    /* Cluster Utilities */
    public Optional<ClusterInfo> describeCluster() {
        try {
            DescribeClusterResult cluster = client.describeCluster();
            String id = cluster.clusterId().get();
            Node nodes = cluster.controller().get();
            Set<AclOperation> acl = cluster.authorizedOperations().get();
            return Optional.of(ClusterInfo.builder()
                    .id(id)
                    .acl(acl)
                    .hostInfo(nodes.toString())
                    .build());
        } catch (InterruptedException | ExecutionException ignored) {
            return Optional.empty();
        }
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
