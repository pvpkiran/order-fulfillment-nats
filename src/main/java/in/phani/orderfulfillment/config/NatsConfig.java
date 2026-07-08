package in.phani.orderfulfillment.config;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Connection setup deliberately includes reconnect/error handling from the
 * start, rather than bolting it on later - this is the piece most demo apps
 * skip, and it's what determines whether in-flight work survives a NATS
 * server restart. The nats.java client already reconnects and resubscribes
 * automatically by default; the listeners below exist so that behavior is
 * visible (logged) rather than silent.
 */
@Configuration
public class NatsConfig {

    private static final Logger log = LoggerFactory.getLogger(NatsConfig.class);

    @Value("${nats.server.url:nats://localhost:4222}")
    private String natsUrl;

    @Bean(destroyMethod = "close")
    public Connection natsConnection() throws Exception {
        Options options = new Options.Builder()
                .server(natsUrl)
                .connectionName("order-fulfillment-app")
                .maxReconnects(-1) // retry indefinitely rather than giving up after N attempts
                .reconnectWait(Duration.ofSeconds(1))
                .connectionListener(this::onConnectionEvent)
                .errorListener(new LoggingErrorListener())
                .build();

        Connection connection = Nats.connect(options);
        log.info("✅ Connected to NATS at {}", natsUrl);
        return connection;
    }

    private void onConnectionEvent(Connection conn, ConnectionListener.Events type) {
        switch (type) {
            case CONNECTED -> log.info("✅ Connected");
            case DISCONNECTED -> log.warn("⚠️  Disconnected from NATS - client will retry automatically");
            case RECONNECTED -> log.info("🔄 Reconnected - subscriptions resume automatically");
            case RESUBSCRIBED -> log.info("🔁 Resubscribed after reconnect");
            case CLOSED -> log.warn("🛑 Connection closed - no further reconnects will be attempted");
            default -> log.debug("NATS connection event: {}", type);
        }
    }

    /**
     * Method set matches jnats 2.25.x. If this doesn't compile against a
     * different client version, check io.nats.client.ErrorListener's javadoc
     * for that version - most methods on this interface have defaults, so
     * only override what you actually want to log.
     */
    private static class LoggingErrorListener implements ErrorListener {
        @Override
        public void errorOccurred(Connection conn, String error) {
            log.error("NATS error: {}", error);
        }

        @Override
        public void exceptionOccurred(Connection conn, Exception exp) {
            log.error("NATS exception", exp);
        }

        @Override
        public void slowConsumerDetected(Connection conn, Consumer consumer) {
            log.warn("NATS slow consumer detected: {}", consumer);
        }
    }
}
