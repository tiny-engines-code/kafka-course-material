package com.chrislomeli.kafka.helloadmin.admin;

import com.chrislomeli.kafka.helloadmin.config.MyConfiguration;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public class AdminExample {

    public static void doExample(String myTopic) {
        try (KafkaAdminUtils clientUtils = new KafkaAdminUtils(MyConfiguration.servers)) {

            // Check the cluster
            log.info("---------Check cluster accessibility -----------");
            Optional<ClusterInfo> clusterInfo = clientUtils.describeCluster();
            if (clusterInfo.isEmpty()) {
                log.error("\tCluster not found at {}", MyConfiguration.servers);
                throw new RuntimeException("Cluster is not available");
            } else {
                ClusterInfo cluster = clusterInfo.get();
                log.info("\tClusterId={}, ACL={} Host={}", cluster.getId(), cluster.getAcl(), cluster.getHostInfo());
            }

            // Find or create a topic
            log.info("---------Find or create topic {} -----------", myTopic);
            if (clientUtils.topicNotExists(myTopic)) {
                log.info("\t*** create a new topic ***");
                clientUtils.saveTopic(myTopic, 2, (short) 1);
            } else {
                log.info("\t*** topic {} already exists ***", myTopic);
            }
        } catch (Exception ignored) {
        }
    }
}
