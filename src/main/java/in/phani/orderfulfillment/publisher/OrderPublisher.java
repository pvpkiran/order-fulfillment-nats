package in.phani.orderfulfillment.publisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import in.phani.orderfulfillment.domain.Order;
import in.phani.orderfulfillment.exception.OrderPublishException;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.Message;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class OrderPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderPublisher.class);

    private final JetStream jetStream;
    private final ObjectMapper objectMapper;

    public OrderPublisher(Connection natsConnection, ObjectMapper objectMapper) throws IOException {
        this.jetStream = natsConnection.jetStream();
        this.objectMapper = objectMapper;
    }

    public PublishAck publishNewOrder(Order order) {
        try {
            byte[] payload = objectMapper.writeValueAsBytes(order);

            Headers headers = new Headers();
            headers.add("Nats-Msg-Id", order.id());

            Message message = NatsMessage.builder()
                    .subject("orders.new")
                    .headers(headers)
                    .data(payload)
                    .build();

            PublishAck ack = jetStream.publish(message);
            log.info("Published order [{}] to stream [{}] seq={} duplicate={}",
                    order.id(), ack.getStream(), ack.getSeqno(), ack.isDuplicate());
            return ack;
        } catch (IOException | JetStreamApiException e) {
            throw new OrderPublishException("Failed to publish order " + order.id(), e);
        }
    }
}