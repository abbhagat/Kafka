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

    public static void createTopics(Properties brokerConfig, List<String> words) {

        CreateTopicsResult createTopicResult;
        AdminClient admin = AdminClient.create(brokerConfig);
        int partitions = 3;
        short replication = 3;

        List<NewTopic> newTopics = words
                .stream()
                .map(topic -> new NewTopic(topic, partitions, replication))
                .collect(Collectors.toList());
        createTopicResult = admin.createTopics(newTopics);
        try {
            createTopicResult.all().get();
            log.info("Topics created successfully");
        } catch (Exception e) {
            log.error("Exception creating topics : {} ", e.getMessage(), e);
        }
    }
}
