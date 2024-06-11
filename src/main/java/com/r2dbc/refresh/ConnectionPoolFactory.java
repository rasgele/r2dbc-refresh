package com.r2dbc.refresh;

import io.r2dbc.spi.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.r2dbc.ConnectionFactoryBuilder;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RefreshScope
public class ConnectionPoolFactory {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ConnectionPoolFactory.class);
    @Bean
    @RefreshScope
    public ConnectionFactory connectionFactory(@Value("${spring.r2dbc.username}") String username,
                                        @Value("${spring.r2dbc.password}") String password,
                                        @Value("${spring.r2dbc.url}") String url) {
        log.info("Refreshing connection factory.");
        return ConnectionFactoryBuilder.withUrl(url).username(username).password(password).build();
    }
}
