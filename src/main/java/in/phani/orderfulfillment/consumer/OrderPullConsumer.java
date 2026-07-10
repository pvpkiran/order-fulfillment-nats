package in.phani.orderfulfillment.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import in.phani.orderfulfillment.config.JetStreamConfig;
import in.phani.orderfulfillment.domain.Order;
import in.phani.orderfulfillment.domain.OrderStatus;
import in.phani.orderfulfillment.kv.OrderStatusStore;
import in.phani.orderfulfillment.objectstore.InvoiceGenerator;
import in.phani.orderfulfillment.objectstore.InvoiceStore;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

/**
 * Wraps a single pull subscription bound to the shared "order-workers"
 * durable consumer. Multiple subscriptions (see OrderWorkerRunner, which
 * opens one per worker) can bind to the SAME durable name concurrently -
 * the server hands out distinct messages across them, which is what gives
 * pull consumers their horizontal-scaling behavior: similar in spirit to a
 * core NATS queue group, but with per-message acknowledgement, redelivery,
 * and durability that queue groups don't have.
 */
@Component
public class OrderPullConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderPullConsumer.class);
    private static final int BATCH_SIZE = 5;
    private static final Duration FETCH_MAX_WAIT = Duration.ofSeconds(2);

    private final JetStream jetStream;
    private final ObjectMapper objectMapper;
    private final OrderStatusStore orderStatusStore;
    private final InvoiceStore invoiceStore;

    public OrderPullConsumer(Connection natsConnection, ObjectMapper objectMapper,
                             OrderStatusStore orderStatusStore, InvoiceStore invoiceStore) throws IOException {
        this.jetStream = natsConnection.jetStream();
        this.objectMapper = objectMapper;
        this.orderStatusStore = orderStatusStore;
        this.invoiceStore = invoiceStore;
    }

    /** Opens a new pull subscription bound to the shared durable consumer. */
    public JetStreamSubscription bindSubscription() throws IOException, JetStreamApiException {
        PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                .stream(JetStreamConfig.ORDERS_STREAM)
                .durable(JetStreamConfig.ORDER_WORKERS_DURABLE)
                .build();
        // The subject argument is left null - the durable consumer's own
        // filterSubject (orders.new, set in JetStreamConfig) already scopes
        // this subscription, so there's nothing extra to specify here.
        return jetStream.subscribe(null, pullOptions);
    }

    /**
     * One fetch-process-ack cycle. Returns how many messages were handled,
     * so a caller can decide whether to loop again immediately (there was
     * work) or pause briefly (there wasn't, no point hammering the server).
     */
    public int pollOnce(JetStreamSubscription subscription, String workerName) {
        List<Message> messages = subscription.fetch(BATCH_SIZE, FETCH_MAX_WAIT);

        for (Message msg : messages) {
            handle(msg, workerName);
        }

        return messages.size();
    }

    private void handle(Message msg, String workerName) {
        long deliveryCount = msg.metaData().deliveredCount();

        try {
            Order order = objectMapper.readValue(msg.getData(), Order.class);
            log.info("⚙️  [{}] Processing order [{}] (delivery #{}): {}",
                    workerName, order.id(), deliveryCount, order.description());

            simulateProcessing(order, deliveryCount);

            // Written BEFORE ack, deliberately: if this throws, it falls
            // into the catch (Exception e) branch below and nak()s rather
            // than acking a message whose status update never landed - so
            // a KV write failure gets retried the same way a processing
            // failure would, instead of silently leaving status stale.
            orderStatusStore.putStatus(order.id(), OrderStatus.PAID);

            // Same before-ack placement and reasoning as the status write
            // above: a failure here also falls into the nak-and-retry path
            // rather than acking an order that never got its invoice.
            invoiceStore.putInvoice(order.id(), InvoiceGenerator.generate(order));

            msg.ack();
            log.info("✅ [{}] Order [{}] processed and acked", workerName, order.id());

        } catch (JsonProcessingException e) {
            // The payload itself is malformed - no number of retries fixes
            // that, so terminate rather than let it keep coming back until
            // maxDeliver quietly exhausts itself.
            log.error("💀 [{}] Unparseable order payload - terminating (poison pill): {}", workerName, e.getMessage());
            msg.term();

        } catch (TransientProcessingException e) {
            // Something recoverable went wrong - nak() tells the server to
            // redeliver immediately rather than waiting out the full
            // ackWait timer, which is what makes the redelivery demo fast.
            log.warn("🔁 [{}] Transient failure, nak'ing order [{}] for redelivery: {}",
                    workerName, e.orderId(), e.getMessage());
            msg.nak();

        } catch (Exception e) {
            log.error("💥 [{}] Unexpected error processing message - nak'ing for redelivery", workerName, e);
            msg.nak();
        }
    }

    /**
     * Demo-only hook: an order whose description contains SIMULATE_FAILURE
     * fails on its first delivery and succeeds on every delivery after -
     * this lets the nak -> redeliver path be exercised deterministically
     * (in a test, or by hand with curl) without needing to actually kill a
     * worker process mid-flight.
     */
    private void simulateProcessing(Order order, long deliveryCount) throws TransientProcessingException {
        if (deliveryCount == 1 && order.description() != null && order.description().contains("SIMULATE_FAILURE")) {
            throw new TransientProcessingException(order.id(), "simulated transient failure on first delivery");
        }
    }

    public static class TransientProcessingException extends Exception {
        private final String orderId;

        public TransientProcessingException(String orderId, String message) {
            super(message);
            this.orderId = orderId;
        }

        public String orderId() {
            return orderId;
        }
    }
}