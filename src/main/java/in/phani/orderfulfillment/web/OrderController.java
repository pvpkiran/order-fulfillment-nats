package in.phani.orderfulfillment.web;

import in.phani.orderfulfillment.domain.Order;
import in.phani.orderfulfillment.publisher.OrderPublisher;
import io.nats.client.api.PublishAck;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Map;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderPublisher orderPublisher;

    public OrderController(OrderPublisher orderPublisher) {
        this.orderPublisher = orderPublisher;
    }

    public record CreateOrderRequest(String customerId, String description, BigDecimal amount) {
    }

    @PostMapping
    public ResponseEntity<?> createOrder(@RequestBody CreateOrderRequest request) {
        Order order = Order.newOrder(request.customerId(), request.description(), request.amount());
        PublishAck ack = orderPublisher.publishNewOrder(order);

        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "orderId", order.id(),
                "streamSequence", ack.getSeqno(),
                "duplicate", ack.isDuplicate()
        ));
    }
}
