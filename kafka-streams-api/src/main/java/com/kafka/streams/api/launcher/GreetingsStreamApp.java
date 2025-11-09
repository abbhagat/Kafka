package com.kafka.streams.api.launcher;

import com.kafka.streams.api.topology.GreetingsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import java.util.List;
import java.util.Properties;
import static com.kafka.streams.api.launcher.TopicUtil.*;

@Slf4j
public class GreetingsStreamApp {

    private static final Properties config = new Properties();

    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "greetings-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        TopicUtil.createTopics(config, List.of(GREETING_TOPIC, GREETING_UPPERCASE_TOPIC, GREETING_SPANISH_TOPIC, WORDS));
    }

    public static void main(String[] args) {
        Topology greetingsTopology = GreetingsTopology.buildCustomTopology();
        KafkaStreams kafkaStreams = new KafkaStreams(greetingsTopology, config);
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        try {
            kafkaStreams.start();
        } catch (Exception e) {
            log.error("Exception in starting stream {}", e.getMessage());
        }
    }
}
