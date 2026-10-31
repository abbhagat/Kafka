package com.kafka.streams.api.domain;

import java.math.BigDecimal;

public record OrderLineItem(String item, Integer count, BigDecimal amount) {}