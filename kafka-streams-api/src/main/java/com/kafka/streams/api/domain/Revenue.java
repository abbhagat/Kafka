package com.kafka.streams.api.domain;

import java.math.BigDecimal;

public record Revenue(String locationId, BigDecimal finalAmount) {}