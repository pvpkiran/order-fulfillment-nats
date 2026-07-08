package in.phani.orderfulfillment.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Deliberately has no status field - "what happened to order X" (this
 * record, as published) and "what's order X's current state" (a KV entry,
 * added in a later phase) are different concerns kept in different places.
 */
public record Order(String id, String customerId, String description, BigDecimal amount, Instant createdAt) {

    public static Order newOrder(String customerId, String description, BigDecimal amount) {
        return new Order(UUID.randomUUID().toString(), customerId, description, amount, Instant.now());
    }
}
