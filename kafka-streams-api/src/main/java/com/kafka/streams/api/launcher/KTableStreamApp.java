package com.kafka.streams.api.launcher;

import com.kafka.streams.api.topology.KTableTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.kafka.streams.api.topology.KTableTopology.WORDS;

@Slf4j
public class KTableStreamApp {

  private static final Properties config = new Properties();

  static {
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
  }

  public static void main(String[] args) {

    Topology kTableTopology = KTableTopology.buildTopology();
    createTopics(config, List.of(WORDS));
    KafkaStreams kafkaStreams = new KafkaStreams(kTableTopology, config);

    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    try {
      kafkaStreams.start();
    } catch (Exception e) {
      log.error("Exception in starting stream {}", e.getMessage());
    }
  }

  private static void createTopics(Properties brokerConfig, List<String> words) {

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
