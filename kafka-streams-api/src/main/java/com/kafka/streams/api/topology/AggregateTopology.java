package com.kafka.streams.api.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

public class AggregateTopology {

    private static final StreamsBuilder streamsBuilder = new StreamsBuilder();

    public static Topology buildTopology() {
        KStream<String, String> aggregateStreams = streamsBuilder.stream("words");
        aggregateStreams.print(Printed.<String,String>toSysOut().withLabel("aggregate"));
        KGroupedStream<String, String> groupedStream = aggregateStreams.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        calculateCount(groupedStream);
        return  streamsBuilder.build();
    }

    private static void calculateCount(KGroupedStream<String, String> groupedStream) {
        KTable<String, Long> countByAlphabet = groupedStream.count(Named.as("count-per-alphabet"));
        countByAlphabet.toStream().print(Printed.<String, Long>toSysOut().withLabel("count-per-alphabet"));
    }
}
