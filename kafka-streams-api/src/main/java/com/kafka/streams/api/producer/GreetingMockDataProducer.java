package com.kafka.streams.api.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafka.streams.api.domain.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.time.LocalDateTime;
import java.util.List;
import static com.kafka.streams.api.producer.ProducerUtil.publishMessageSync;


@Slf4j
public class GreetingMockDataProducer {

    private static final String GREETINGS = "greetings";
    private static final ObjectMapper objectMapper = new ObjectMapper()
                                                    .registerModule(new JavaTimeModule())
                                                    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    public static void main(String[] args) {
        englishGreetings();
        spanishGreetings();
    }

    private static void spanishGreetings() {
        var spanishGreetings = List.of(
                                        new Greeting("Hello, Good Morning!", LocalDateTime.now()),
                                        new Greeting("Hello, Good Evening!", LocalDateTime.now()),
                                        new Greeting("Hello, Good Night!",   LocalDateTime.now())
                                      );
        spanishGreetings
                .forEach(greeting -> {
                    try {
                        var greetingJSON = objectMapper.writeValueAsString(greeting);
                        var recordMetaData = publishMessageSync(GREETINGS, null, greetingJSON);
                        log.info("Published the alphabet message : {} ", recordMetaData);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static void englishGreetings() {
        var spanishGreetings = List.of(
                                        new Greeting("¡Hola buenos dias!", LocalDateTime.now()),
                                        new Greeting("¡Hola buenas tardes!", LocalDateTime.now()),
                                        new Greeting("¡Hola, buenas noches!", LocalDateTime.now())
                                      );
        spanishGreetings
                .forEach(greeting -> {
                    try {
                        String greetingJSON = objectMapper.writeValueAsString(greeting);
                        RecordMetadata recordMetaData = publishMessageSync(GREETINGS, null, greetingJSON);
                        log.info("Published the alphabet message : {} ", recordMetaData);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

}
