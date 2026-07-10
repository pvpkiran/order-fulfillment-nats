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

    public JetStreamSubscription bindSubscription() throws IOException, JetStreamApiException {
        PullSubscribeOptions pullOptions = PullSubscribeOptions.builder()
                .stream(JetStreamConfig.ORDERS_STREAM)
                .durable(JetStreamConfig.ORDER_WORKERS_DURABLE)
                .build();
        return jetStream.subscribe(null, pullOptions);
    }

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
            log.info("[{}] Processing order [{}] (delivery #{}): {}",
                    workerName, order.id(), deliveryCount, order.description());

            simulateProcessing(order, deliveryCount);

            orderStatusStore.putStatus(order.id(), OrderStatus.PAID);
            invoiceStore.putInvoice(order.id(), InvoiceGenerator.generate(order));

            msg.ack();
            log.info("[{}] Order [{}] processed and acked", workerName, order.id());

        } catch (JsonProcessingException e) {
            log.error("[{}] Unparseable order payload - terminating: {}", workerName, e.getMessage());
            msg.term();

        } catch (TransientProcessingException e) {
            log.warn("[{}] Transient failure, nak'ing order [{}] for redelivery: {}",
                    workerName, e.orderId(), e.getMessage());
            msg.nak();

        } catch (Exception e) {
            log.error("[{}] Unexpected error processing message - nak'ing for redelivery", workerName, e);
            msg.nak();
        }
    }

    /**
     * An order whose description contains SIMULATE_FAILURE fails on its
     * first delivery and succeeds on redelivery - lets the nak/redeliver
     * path be exercised on demand without killing a worker process.
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