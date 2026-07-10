package in.phani.orderfulfillment.consumer;

import io.nats.client.JetStreamSubscription;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class OrderWorkerRunner {

    private static final Logger log = LoggerFactory.getLogger(OrderWorkerRunner.class);

    @Value("${nats.orders.worker-count:3}")
    private int workerCount;

    private final OrderPullConsumer orderPullConsumer;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final List<Thread> workerThreads = new ArrayList<>();

    public OrderWorkerRunner(OrderPullConsumer orderPullConsumer) {
        this.orderPullConsumer = orderPullConsumer;
    }

    @PostConstruct
    public void start() {
        for (int i = 1; i <= workerCount; i++) {
            String workerName = "Worker-" + i;
            Thread worker = Thread.ofVirtual().name(workerName).start(() -> runWorkerLoop(workerName));
            workerThreads.add(worker);
        }
        log.info("Started {} order worker(s) bound to durable consumer [order-workers]", workerCount);
    }

    private void runWorkerLoop(String workerName) {
        try {
            JetStreamSubscription subscription = orderPullConsumer.bindSubscription();
            log.info("[{}] Bound to durable consumer, polling for orders...", workerName);

            while (running.get()) {
                int handled = orderPullConsumer.pollOnce(subscription, workerName);
                if (handled == 0) {
                    Thread.sleep(500);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("[{}] Interrupted, shutting down", workerName);
        } catch (Exception e) {
            log.error("[{}] Worker loop failed unexpectedly", workerName, e);
        }
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        workerThreads.forEach(Thread::interrupt);
        log.info("Signalled {} order worker(s) to stop", workerThreads.size());
    }
}