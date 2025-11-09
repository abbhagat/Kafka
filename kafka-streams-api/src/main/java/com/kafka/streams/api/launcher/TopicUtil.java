package com.kafka.streams.api.launcher;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class TopicUtil {

    private static final int numPartitions = 3;
    private static final short replicationFactor = 3;

    public static void createTopics(Properties brokerConfig, List<String> words) {
        try (AdminClient admin = AdminClient.create(brokerConfig)) {
            List<NewTopic> newTopics = words
                                            .stream()
                                            .map(topic -> new NewTopic(topic, numPartitions, replicationFactor))
                                            .collect(Collectors.toList());
            CreateTopicsResult createTopicResult = admin.createTopics(newTopics);
            try {
                createTopicResult.all().get();
                log.info("Topics created successfully");
            } catch (Exception e) {
                log.error("Exception creating topics : {} ", e.getMessage(), e);
            }
        }
    }
}
