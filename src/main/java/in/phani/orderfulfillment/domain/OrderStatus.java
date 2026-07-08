package in.phani.orderfulfillment.domain;

/**
 * Not used until Phase 3 (KV store) - scaffolded now since it's part of
 * the domain vocabulary. Each value here becomes a value written to the
 * order-status KV bucket, not a field on Order itself.
 */
public enum OrderStatus {
    PENDING,
    PAID,
    SHIPPED,
    CANCELLED
}
