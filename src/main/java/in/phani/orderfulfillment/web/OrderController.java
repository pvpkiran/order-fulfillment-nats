package in.phani.orderfulfillment.web;

import in.phani.orderfulfillment.domain.Order;
import in.phani.orderfulfillment.domain.OrderStatus;
import in.phani.orderfulfillment.kv.OrderStatusStore;
import in.phani.orderfulfillment.objectstore.InvoiceStore;
import in.phani.orderfulfillment.publisher.OrderPublisher;
import io.nats.client.api.PublishAck;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
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
    private final OrderStatusStore orderStatusStore;
    private final InvoiceStore invoiceStore;

    public OrderController(OrderPublisher orderPublisher, OrderStatusStore orderStatusStore, InvoiceStore invoiceStore) {
        this.orderPublisher = orderPublisher;
        this.orderStatusStore = orderStatusStore;
        this.invoiceStore = invoiceStore;
    }

    public record CreateOrderRequest(String customerId, String description, BigDecimal amount) {
    }

    @PostMapping
    public ResponseEntity<?> createOrder(@RequestBody CreateOrderRequest request) {
        Order order = Order.newOrder(request.customerId(), request.description(), request.amount());
        PublishAck ack = orderPublisher.publishNewOrder(order);

        // Written here, synchronously, right after the publish is confirmed -
        // so a GET /orders/{id}/status immediately after this returns PENDING
        // rather than 404, even before any worker has picked the order up.
        orderStatusStore.putStatus(order.id(), OrderStatus.PENDING);

        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "orderId", order.id(),
                "streamSequence", ack.getSeqno(),
                "duplicate", ack.isDuplicate()
        ));
    }

    @GetMapping("/{orderId}/status")
    public ResponseEntity<?> getOrderStatus(@PathVariable String orderId) {
        return orderStatusStore.getStatus(orderId)
                .<ResponseEntity<?>>map(status -> ResponseEntity.ok(Map.of("orderId", orderId, "status", status)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/{orderId}/invoice")
    public ResponseEntity<byte[]> getInvoice(@PathVariable String orderId) {
        return invoiceStore.getInvoice(orderId)
                .map(bytes -> ResponseEntity.ok()
                        .contentType(MediaType.TEXT_PLAIN)
                        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"invoice-" + orderId + ".txt\"")
                        .body(bytes))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}