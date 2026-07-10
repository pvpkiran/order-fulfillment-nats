package in.phani.orderfulfillment.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record Order(String id, String customerId, String description, BigDecimal amount, Instant createdAt) {

    public static Order newOrder(String customerId, String description, BigDecimal amount) {
        return new Order(UUID.randomUUID().toString(), customerId, description, amount, Instant.now());
    }
}