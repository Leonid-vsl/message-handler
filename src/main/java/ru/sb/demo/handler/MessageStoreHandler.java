package ru.sb.demo.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import ru.sb.demo.model.Message;
import ru.sb.demo.service.MessageService;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class MessageStoreHandler {

    private static final Logger logger = LogManager.getLogger(MessageStoreHandler.class);

    private final MessageService messageService;

    public MessageStoreHandler(MessageService messageService) {
        this.messageService = messageService;
    }

    @KafkaListener(
            id = "message-store-handler",
            topics = "${app.handledMessageTopic}",
            containerFactory = "messageStoreContainerFactory"
    )
    public void handleMessages(@Payload List<ConsumerRecord<Long, Message>> messages) {
        logger.info("accept messages: {}", messages.stream().map(r -> r.value().getMessageId()).collect(toList()));

        List<Message> batch = messages.stream().map(ConsumerRecord::value).collect(toList());

        logger.info("Sending to storage batch with size {}", batch.size());
        logger.info("Sending to storage messages with id's {}",
                batch.stream().map(Message::getMessageId).collect(toList()));
        messageService.handleMessages(batch);
        logger.info("Message batch handled.");
    }
}
