package in.phani.orderfulfillment.config;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.KeyValueManagement;
import io.nats.client.ObjectStoreManagement;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.ObjectStoreConfiguration;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;

/**
 * Creates the ORDERS stream, its durable pull consumer, the order-status KV
 * bucket, and the invoices Object Store bucket on startup if they don't
 * already exist.
 */
@Component
public class JetStreamConfig {

    private static final Logger log = LoggerFactory.getLogger(JetStreamConfig.class);

    public static final String ORDERS_STREAM = "ORDERS";
    public static final String ORDERS_SUBJECTS = "orders.*";

    public static final String ORDER_WORKERS_DURABLE = "order-workers";
    public static final String NEW_ORDER_SUBJECT = "orders.new";
    private static final long MAX_DELIVER = 5;

    public static final String ORDER_STATUS_BUCKET = "order-status";
    public static final String INVOICES_BUCKET = "invoices";

    @Value("${nats.orders.ack-wait-seconds:10}")
    private long ackWaitSeconds;

    private final Connection natsConnection;

    public JetStreamConfig(Connection natsConnection) {
        this.natsConnection = natsConnection;
    }

    @PostConstruct
    public void bootstrap() throws IOException, JetStreamApiException {
        JetStreamManagement jsm = natsConnection.jetStreamManagement();
        bootstrapOrdersStream(jsm);
        bootstrapOrderWorkersConsumer(jsm);
        bootstrapOrderStatusBucket();
        bootstrapInvoicesBucket();
    }

    private void bootstrapOrdersStream(JetStreamManagement jsm) throws IOException, JetStreamApiException {
        if (jsm.getStreamNames().contains(ORDERS_STREAM)) {
            log.info("Stream [{}] already exists", ORDERS_STREAM);
            return;
        }

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(ORDERS_STREAM)
                .subjects(ORDERS_SUBJECTS)
                .storageType(StorageType.File)
                .retentionPolicy(RetentionPolicy.WorkQueue)
                .build();

        jsm.addStream(streamConfig);
        log.info("Created stream [{}] on subjects [{}] with WorkQueue retention", ORDERS_STREAM, ORDERS_SUBJECTS);
    }

    private void bootstrapOrderWorkersConsumer(JetStreamManagement jsm) throws IOException, JetStreamApiException {
        ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                .durable(ORDER_WORKERS_DURABLE)
                .filterSubject(NEW_ORDER_SUBJECT)
                .ackPolicy(AckPolicy.Explicit)
                .ackWait(Duration.ofSeconds(ackWaitSeconds))
                .maxDeliver(MAX_DELIVER)
                .build();

        jsm.addOrUpdateConsumer(ORDERS_STREAM, consumerConfig);
        log.info("Durable consumer [{}] ready on subject [{}] (ackWait={}s, maxDeliver={})",
                ORDER_WORKERS_DURABLE, NEW_ORDER_SUBJECT, ackWaitSeconds, MAX_DELIVER);
    }

    private void bootstrapOrderStatusBucket() throws IOException, JetStreamApiException {
        KeyValueManagement kvm = natsConnection.keyValueManagement();

        if (kvm.getBucketNames().contains(ORDER_STATUS_BUCKET)) {
            log.info("KV bucket [{}] already exists", ORDER_STATUS_BUCKET);
            return;
        }

        KeyValueConfiguration kvConfig = KeyValueConfiguration.builder()
                .name(ORDER_STATUS_BUCKET)
                .storageType(StorageType.File)
                .build();

        kvm.create(kvConfig);
        log.info("Created KV bucket [{}]", ORDER_STATUS_BUCKET);
    }

    private void bootstrapInvoicesBucket() throws IOException, JetStreamApiException {
        ObjectStoreManagement osm = natsConnection.objectStoreManagement();

        if (osm.getBucketNames().contains(INVOICES_BUCKET)) {
            log.info("Object Store bucket [{}] already exists", INVOICES_BUCKET);
            return;
        }

        ObjectStoreConfiguration osConfig = ObjectStoreConfiguration.builder()
                .name(INVOICES_BUCKET)
                .storageType(StorageType.File)
                .build();

        osm.create(osConfig);
        log.info("Created Object Store bucket [{}]", INVOICES_BUCKET);
    }
}