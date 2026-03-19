package ru.sb.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.sb.demo.service.MessageService;
import ru.sb.demo.service.MessageServiceImpl;

@Configuration
public class ServiceConfig {

    @Bean
    public MessageService messageService(JdbcTemplate jdbcTemplate) {
        return new MessageServiceImpl(jdbcTemplate);
    }
}
