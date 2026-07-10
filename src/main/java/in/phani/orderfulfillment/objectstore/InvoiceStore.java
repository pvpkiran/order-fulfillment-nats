package in.phani.orderfulfillment.objectstore;

import in.phani.orderfulfillment.config.JetStreamConfig;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.ObjectStore;
import io.nats.client.api.ObjectInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;

@Component
public class InvoiceStore {

    private static final Logger log = LoggerFactory.getLogger(InvoiceStore.class);

    private final ObjectStore invoiceStore;

    public InvoiceStore(Connection natsConnection) throws IOException {
        this.invoiceStore = natsConnection.objectStore(JetStreamConfig.INVOICES_BUCKET);
    }

    public void putInvoice(String orderId, byte[] pdfBytes) {
        try {
            ObjectInfo info = invoiceStore.put(orderId, pdfBytes);
            log.info("Stored invoice for order [{}] ({} bytes, {} chunk(s))",
                    orderId, info.getSize(), info.getChunks());
        } catch (IOException | JetStreamApiException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed to store invoice for order " + orderId, e);
        }
    }

    public Optional<byte[]> getInvoice(String orderId) {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            invoiceStore.get(orderId, buffer);
            return Optional.of(buffer.toByteArray());
        } catch (JetStreamApiException e) {
            return Optional.empty();
        } catch (IOException | InterruptedException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed to read invoice for order " + orderId, e);
        }
    }
}