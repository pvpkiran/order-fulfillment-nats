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

/**
 * Starts N worker threads, each binding its own subscription to the same
 * "order-workers" durable consumer. Virtual threads are used since these
 * loops spend nearly all their time blocked on fetch() waiting on the
 * network, not doing CPU work - exactly the workload virtual threads are
 * for. All N workers pulling from the same durable name is what load-
 * balances orders across them: the server won't hand the same message to
 * two workers at once.
 */
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
        log.info("✅ Started {} order worker(s) bound to durable consumer [order-workers]", workerCount);
    }

    private void runWorkerLoop(String workerName) {
        try {
            JetStreamSubscription subscription = orderPullConsumer.bindSubscription();
            log.info("🔗 [{}] Bound to durable consumer, polling for orders...", workerName);

            while (running.get()) {
                int handled = orderPullConsumer.pollOnce(subscription, workerName);
                if (handled == 0) {
                    // Nothing was waiting - a short pause so an idle worker
                    // isn't hammering the server with back-to-back fetches.
                    Thread.sleep(500);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("🛑 [{}] Interrupted, shutting down", workerName);
        } catch (Exception e) {
            log.error("💥 [{}] Worker loop failed unexpectedly", workerName, e);
        }
    }

    /**
     * Signals every worker to stop after its current fetch cycle completes
     * (at most FETCH_MAX_WAIT, currently 2s) rather than killing threads
     * outright - so a message a worker is mid-processing on doesn't get
     * abandoned in an inconsistent state.
     */
    @PreDestroy
    public void stop() {
        running.set(false);
        workerThreads.forEach(Thread::interrupt);
        log.info("🛑 Signalled {} order worker(s) to stop", workerThreads.size());
    }
}