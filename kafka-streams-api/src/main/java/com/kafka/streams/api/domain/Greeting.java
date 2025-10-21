package com.kafka.streams.api.domain;

import java.time.LocalDateTime;

public record Greeting(String message, LocalDateTime timestamp) {}
