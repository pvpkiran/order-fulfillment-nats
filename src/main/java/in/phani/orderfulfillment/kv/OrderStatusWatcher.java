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

@Component
public class OrderStatusWatcher {

    private static final Logger log = LoggerFactory.getLogger(OrderStatusWatcher.class);

    private final Connection natsConnection;

    // Not opened in try-with-resources: this subscription stays open for
    // the app's lifetime and is closed explicitly in stop() below.
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
                log.info("[Watcher] order [{}] -> {}", entry.getKey(), entry.getValueAsString());
            }

            @Override
            public void endOfData() {
                log.info("[Watcher] caught up with existing entries - now watching live");
            }
        });

        log.info("OrderStatusWatcher subscribed to bucket [{}]", JetStreamConfig.ORDER_STATUS_BUCKET);
    }

    @PreDestroy
    public void stop() throws Exception {
        if (watchSubscription != null) {
            watchSubscription.close();
            log.info("OrderStatusWatcher unsubscribed from bucket [{}]", JetStreamConfig.ORDER_STATUS_BUCKET);
        }
    }
}