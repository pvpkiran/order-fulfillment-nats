package in.phani.orderfulfillment.kv;

import in.phani.orderfulfillment.config.JetStreamConfig;
import in.phani.orderfulfillment.domain.OrderStatus;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

@Component
public class OrderStatusStore {

    private static final Logger log = LoggerFactory.getLogger(OrderStatusStore.class);

    private final KeyValue orderStatusKv;

    public OrderStatusStore(Connection natsConnection) throws IOException {
        this.orderStatusKv = natsConnection.keyValue(JetStreamConfig.ORDER_STATUS_BUCKET);
    }

    public void putStatus(String orderId, OrderStatus status) {
        try {
            long revision = orderStatusKv.put(orderId, status.name().getBytes(StandardCharsets.UTF_8));
            log.info("Order [{}] status -> {} (revision {})", orderId, status, revision);
        } catch (IOException | JetStreamApiException e) {
            throw new IllegalStateException("Failed to update status for order " + orderId, e);
        }
    }

    public Optional<OrderStatus> getStatus(String orderId) {
        try {
            KeyValueEntry entry = orderStatusKv.get(orderId);
            if (entry == null) {
                return Optional.empty();
            }
            return Optional.of(OrderStatus.valueOf(entry.getValueAsString()));
        } catch (IOException | JetStreamApiException e) {
            throw new IllegalStateException("Failed to read status for order " + orderId, e);
        }
    }
}