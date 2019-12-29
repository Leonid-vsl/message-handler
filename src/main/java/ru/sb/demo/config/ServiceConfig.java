package ru.sb.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.sb.demo.repository.MessageRepository;
import ru.sb.demo.service.MessageService;
import ru.sb.demo.service.MessageServiceImpl;

@Configuration
public class ServiceConfig {

    @Bean
    public MessageService messageService(MessageRepository messageRepository) {
        return new MessageServiceImpl(messageRepository);
    }
}
