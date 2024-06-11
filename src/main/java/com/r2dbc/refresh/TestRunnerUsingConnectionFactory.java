package com.r2dbc.refresh;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Statement;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.context.scope.refresh.RefreshScope;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@EnableScheduling
@Component
public class TestRunnerUsingConnectionFactory {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TestRunnerUsingConnectionFactory.class);
    private final ConnectionFactory connectionFactory;
    private final RefreshScope refreshScope;

    public TestRunnerUsingConnectionFactory(ConnectionFactory connectionFactory, RefreshScope refreshScope) {
        this.connectionFactory = connectionFactory;
        this.refreshScope = refreshScope;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void createEntities() {
        var counter = new AtomicInteger();
        var retrySpec = Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofMillis(100))
                .transientErrors(true)
                .doBeforeRetry(retrySignal -> log.warn("Failed {} in row. Retrying {}",
                        retrySignal.totalRetriesInARow(), retrySignal.failure().getMessage()));

        initDB().thenMany(Flux.range(0, Integer.MAX_VALUE))
                .flatMap(index -> persist().thenReturn(index))
                .retryWhen(retrySpec)
                .doOnNext(unused -> logCounter(counter))
                .subscribe();
    }

    private static void logCounter(AtomicInteger counter) {
        if (counter.incrementAndGet() % 1000 == 0) log.info(String.format("Wrote: %16d", counter.get()));
    }

    private Mono<Void> persist() {
        return Mono.from(connectionFactory.create()).flatMap(connection ->
                Mono.from(connection.createStatement("INSERT INTO entry (id, name) VALUES ($1, $2)")
                                .bind("$1", UUID.randomUUID().toString())
                                .bind("$2", "name")
                                .execute())
                        .flatMap(result -> Mono.from(result.getRowsUpdated()))
                        .then(Mono.from(connection.close()))
        );
    }

    private Mono<Void> initDB() {
        return Mono.from(connectionFactory.create()).flatMap(connection ->
                Mono.from(getCreateStatement(connection).execute())
                        .flatMap(result -> Mono.from(result.getRowsUpdated()))
                        .then(Mono.from(connection.close()))
                        .doOnTerminate(() -> log.info("Table created"))
        );
    }

    private Statement getCreateStatement(Connection connection) {
        return connection.createStatement("CREATE TABLE IF NOT EXISTS entry (id VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL)");
    }

    @Scheduled(fixedRate = 5000)
    public void refresh() {
        log.info("Refreshing all beans...");
        refreshScope.refreshAll();
        log.info("Refreshed all beans!");
    }
}
