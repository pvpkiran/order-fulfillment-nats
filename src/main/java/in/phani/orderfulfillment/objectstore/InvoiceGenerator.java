package in.phani.orderfulfillment.objectstore;

import in.phani.orderfulfillment.domain.Order;

import java.nio.charset.StandardCharsets;

/**
 * Produces a plain-text invoice. Kept separate from InvoiceStore so a real
 * PDF library could replace this without touching how invoices are stored.
 */
public final class InvoiceGenerator {

    private InvoiceGenerator() {
    }

    public static byte[] generate(Order order) {
        String text = """
                INVOICE
                =======
                Order ID:    %s
                Customer:    %s
                Description: %s
                Amount:      %s
                Created:     %s
                """.formatted(
                order.id(),
                order.customerId(),
                order.description(),
                order.amount(),
                order.createdAt()
        );
        return text.getBytes(StandardCharsets.UTF_8);
    }
}