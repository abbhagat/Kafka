package com.kafka.streams.api.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;

public class KTableTopology {

    private static final String WORDS = "words";

    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KTable<String, String> wordsTable = streamsBuilder.table(WORDS, Consumed.with(Serdes.String(), Serdes.String()));
        wordsTable.filter((key,value)-> value.length() > 2)
                  .toStream()
                  .print(Printed.<String, String>toSysOut().withLabel("words-ktable"));
        return  streamsBuilder.build();
    }
}
