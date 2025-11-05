package com.kafka.streams.api.launcher;

import com.kafka.streams.api.topology.GreetingsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.kafka.streams.api.topology.GreetingsTopology.*;

@Slf4j
public class GreetingsStreamApp {

  public static void main(String[] args) {

    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "greetings-app");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    createTopics(properties, List.of(GREETING, GREETING_UPPERCASE, GREETING_SPANISH));

    Topology greetingsTopology = GreetingsTopology.buildCustomTopology();

    KafkaStreams kafkaStreams = new KafkaStreams(greetingsTopology, properties);

    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    try {
      kafkaStreams.start();
    } catch (Exception e) {
      log.error("Exception in starting stream {}", e.getMessage());
    }
  }

  private static void createTopics(Properties brokerConfig, List<String> greetings) {

    AdminClient admin = AdminClient.create(brokerConfig);
    int partitions = 3;
    short replication = 3;

    List<NewTopic> newTopics = greetings
                                        .stream()
                                        .map(topic -> new NewTopic(topic, partitions, replication))
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
