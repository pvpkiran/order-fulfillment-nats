package in.phani.orderfulfillment.kv;

import in.phani.orderfulfillment.config.JetStreamConfig;
import io.nats.client.Connection;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueWatcher;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Stand-in for a live dashboard: subscribes to every change on the
 * order-status bucket and logs it the instant it happens, via watchAll()
 * rather than polling for it. This is push, not pull - the opposite access
 * pattern from OrderStatusStore.getStatus(), which is a one-off lookup a
 * caller makes on demand.
 */
@Component
public class OrderStatusWatcher {

    private static final Logger log = LoggerFactory.getLogger(OrderStatusWatcher.class);

    private final Connection natsConnection;

    // Deliberately NOT opened in try-with-resources: this subscription is
    // meant to stay open for the app's entire lifetime, not close the
    // instant startWatching() returns. It's closed explicitly in stop()
    // below instead, on actual application shutdown.
    private io.nats.client.impl.NatsKeyValueWatchSubscription watchSubscription;

    public OrderStatusWatcher(Connection natsConnection) {
        this.natsConnection = natsConnection;
    }

    @PostConstruct
    public void startWatching() throws Exception {
        KeyValue orderStatusKv = natsConnection.keyValue(JetStreamConfig.ORDER_STATUS_BUCKET);

        watchSubscription = orderStatusKv.watchAll(new KeyValueWatcher() {
            @Override
            public void watch(KeyValueEntry entry) {
                log.info("📡 [Watcher] order [{}] -> {}", entry.getKey(), entry.getValueAsString());
            }

            @Override
            public void endOfData() {
                // Fired once after replaying whatever already existed in the
                // bucket at subscribe time - everything after this call is a
                // genuinely new, live change rather than catch-up history.
                log.info("📡 [Watcher] caught up with existing entries - now watching live");
            }
        });

        log.info("✅ OrderStatusWatcher subscribed to bucket [{}]", JetStreamConfig.ORDER_STATUS_BUCKET);
    }

    @PreDestroy
    public void stop() throws Exception {
        if (watchSubscription != null) {
            watchSubscription.close();
            log.info("🛑 OrderStatusWatcher unsubscribed from bucket [{}]", JetStreamConfig.ORDER_STATUS_BUCKET);
        }
    }
}