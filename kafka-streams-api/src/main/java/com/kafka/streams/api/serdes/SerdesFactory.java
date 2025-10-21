package com.kafka.streams.api.serdes;

import com.kafka.streams.api.domain.Greeting;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    public static Serde<Greeting> greetingSerdes() {
        return new GreetingSerdes();
    }



}
