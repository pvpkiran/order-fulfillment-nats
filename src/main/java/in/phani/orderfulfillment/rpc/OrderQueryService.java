package in.phani.orderfulfillment.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import in.phani.orderfulfillment.domain.OrderStatus;
import in.phani.orderfulfillment.kv.OrderStatusStore;
import io.nats.client.Connection;
import io.nats.service.Service;
import io.nats.service.ServiceEndpoint;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Exposes order.get and order.cancel as a discoverable NATS service.
 * Does not replace OrderController's direct reads from OrderStatusStore -
 * this is a second, independent access path for callers that speak NATS
 * directly rather than HTTP.
 */
@Component
public class OrderQueryService {

    private static final Logger log = LoggerFactory.getLogger(OrderQueryService.class);

    @Value("${nats.orders.query-service-instance-count:3}")
    private int instanceCount;

    private final Connection natsConnection;
    private final OrderStatusStore orderStatusStore;
    private final ObjectMapper objectMapper;
    private final List<Service> services = new ArrayList<>();

    public OrderQueryService(Connection natsConnection, OrderStatusStore orderStatusStore, ObjectMapper objectMapper) {
        this.natsConnection = natsConnection;
        this.orderStatusStore = orderStatusStore;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void start() throws IOException {
        for (int i = 1; i <= instanceCount; i++) {
            ServiceEndpoint getEndpoint = ServiceEndpoint.builder()
                    .endpointName("order-get")
                    .endpointSubject("order.get")
                    .handler(msg -> msg.respond(natsConnection, handleGet(msg.getData())))
                    .build();

            ServiceEndpoint cancelEndpoint = ServiceEndpoint.builder()
                    .endpointName("order-cancel")
                    .endpointSubject("order.cancel")
                    .handler(msg -> msg.respond(natsConnection, handleCancel(msg.getData())))
                    .build();

            Service service = Service.builder()
                    .connection(natsConnection)
                    .name("order-query-service")
                    .version("1.0.0")
                    .description("Looks up and cancels orders via the order-status KV bucket")
                    .addServiceEndpoint(getEndpoint)
                    .addServiceEndpoint(cancelEndpoint)
                    .build();

            service.startService();
            services.add(service);
        }

        log.info("Started {} instance(s) of order-query-service (order.get, order.cancel)", instanceCount);
    }

    private byte[] handleGet(byte[] requestData) {
        String orderId = new String(requestData, StandardCharsets.UTF_8).trim();
        try {
            Optional<OrderStatus> status = orderStatusStore.getStatus(orderId);
            if (status.isEmpty()) {
                return toJsonBytes(Map.of("error", "order not found", "orderId", orderId));
            }
            return toJsonBytes(Map.of("orderId", orderId, "status", status.get()));
        } catch (Exception e) {
            log.error("order.get failed for [{}]", orderId, e);
            return toJsonBytes(Map.of("error", "internal error", "orderId", orderId));
        }
    }

    private byte[] handleCancel(byte[] requestData) {
        String orderId = new String(requestData, StandardCharsets.UTF_8).trim();
        try {
            orderStatusStore.putStatus(orderId, OrderStatus.CANCELLED);
            return toJsonBytes(Map.of("orderId", orderId, "status", OrderStatus.CANCELLED));
        } catch (Exception e) {
            log.error("order.cancel failed for [{}]", orderId, e);
            return toJsonBytes(Map.of("error", "internal error", "orderId", orderId));
        }
    }

    private byte[] toJsonBytes(Object value) {
        try {
            return objectMapper.writeValueAsBytes(value);
        } catch (IOException e) {
            return "{\"error\":\"failed to serialize response\"}".getBytes(StandardCharsets.UTF_8);
        }
    }

    @PreDestroy
    public void stop() {
        services.forEach(Service::stop);
        log.info("Stopped {} order-query-service instance(s)", services.size());
    }
}