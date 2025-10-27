package com.kafka.streams.api.producer;

import lombok.extern.slf4j.Slf4j;

import static com.kafka.streams.api.producer.ProducerUtil.publishMessageSync;

@Slf4j
public class WordsProducer {

  private static final String WORDS = "words";

  public static void main(String[] args) {

    var key = "A";

    var word = "Apple";
    var word1 = "Alligator";
    var word2 = "Ambulance";

    var recordMetaData = publishMessageSync(WORDS, key, word);
    log.info("Published the alphabet message : {} ", recordMetaData);

    var recordMetaData1 = publishMessageSync(WORDS, key, word1);
    log.info("Published the alphabet message : {} ", recordMetaData1);

    var recordMetaData2 = publishMessageSync(WORDS, key, word2);
    log.info("Published the alphabet message : {} ", recordMetaData2);

    var bKey = "B";

    var bWord1 = "Bus";
    var bWord2 = "Baby";
    var recordMetaData3 = publishMessageSync(WORDS, bKey, bWord1);
    log.info("Published the alphabet message : {} ", recordMetaData2);

    var recordMetaData4 = publishMessageSync(WORDS, bKey, bWord2);
    log.info("Published the alphabet message : {} ", recordMetaData2);

  }

}
