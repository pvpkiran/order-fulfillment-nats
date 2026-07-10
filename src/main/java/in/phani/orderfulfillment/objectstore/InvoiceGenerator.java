package in.phani.orderfulfillment.objectstore;

import in.phani.orderfulfillment.domain.Order;

import java.nio.charset.StandardCharsets;

/**
 * Produces a deliberately simple, plain-text invoice. The point of this
 *is exercising Object Store put/get with a real blob, not building
 * a PDF renderer - hand-assembling a byte-correct PDF (xref table, object
 * offsets, etc.) without a way to render and verify it is an easy place to
 * ship something subtly broken, so this stays honest about being a stub.
 *
 * A real system would swap this out for an actual PDF library (PDFBox,
 * iText) without touching InvoiceStore or anything downstream of it -
 * that's the point of keeping "generate the content" and "store the
 * content" as separate concerns.
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