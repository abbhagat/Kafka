package com.kafka.streams.api.producer;

import lombok.extern.slf4j.Slf4j;
import static com.kafka.streams.api.producer.ProducerUtil.publishMessageSync;

@Slf4j
public class WordsProducer {

    private static final String WORDS = "words";

    public static void main(String[] args) {

        String key   = "A";
        String word1 = "Apple";
        String word2 = "Alligator";
        String word3 = "Ambulance";

        log.info("Published the alphabet message : {} ", publishMessageSync(WORDS, key, word1));
        log.info("Published the alphabet message : {} ", publishMessageSync(WORDS, key, word2));
        log.info("Published the alphabet message : {} ", publishMessageSync(WORDS, key, word3));


        key   = "B";
        String word4 = "Bus";
        String word5 = "Baby";

        log.info("Published the alphabet message : {} ", publishMessageSync(WORDS, key, word4));
        log.info("Published the alphabet message : {} ", publishMessageSync(WORDS, key, word5));
    }

}
