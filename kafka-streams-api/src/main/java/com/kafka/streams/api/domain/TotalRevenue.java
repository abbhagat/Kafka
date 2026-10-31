package com.kafka.streams.api.domain;

import java.math.BigDecimal;

public record TotalRevenue(String locationId, Integer runningOrderCount, BigDecimal runningRevenue) {}