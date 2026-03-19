package ru.sb.demo.handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.util.concurrent.ListenableFuture;
import java.util.concurrent.CompletableFuture;
import ru.sb.demo.model.Message;
import ru.sb.demo.model.MessageBatch;

import java.util.List;
import java.util.stream.Collectors;

public class IncomingMessageHandler extends AbstractConsumerSeekAware {

    private static final Logger logger = LogManager.getLogger(IncomingMessageHandler.class);

    private final String handledMessageTopic;
    private final KafkaTemplate<Long, Message> handledMessageTemplate;

    public IncomingMessageHandler(
            String handledMessageTopic,
            KafkaTemplate<Long, Message> handledMessageTemplate
    ) {
        this.handledMessageTopic = handledMessageTopic;
        this.handledMessageTemplate = handledMessageTemplate;
    }

    @KafkaListener(
            id = "incoming-message-handler",
            topics = "${app.incomingMessageTopic}",
            containerFactory = "incomingMessageContainerFactory"
    )
    public void handleIncomingMessage(@Payload List<MessageBatch> batch) {

        logger.debug("Received new message batch of size {}", batch.size());

        if (batch.isEmpty()) {
            logger.warn("Messages payload batch is empty");
            return;
        }
        
        List<CompletableFuture<SendResult<Long, Message>>> futures = batch.stream()
                .filter(this::isNotEmptyBatch)
                .flatMap(payload -> payload.getMessages().stream())
                .filter(this::validateMessage)
                .map(message -> handledMessageTemplate.send(
                        handledMessageTopic,
                        message.getMessageId(),
                        message
                ).completable())
                .collect(Collectors.toList());
                
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            logger.error("Failed to send message batch to Kafka. Aborting to prevent data loss.", e);
            throw new RuntimeException("Failed to send message batch to Kafka", e);
        }

    }

    private boolean isNotEmptyBatch(MessageBatch payload) {
        boolean isNotEmpty = !payload.getMessages().isEmpty();
        if (!isNotEmpty) {
            logger.warn("Batch is empty");
        }
        return isNotEmpty;
    }

    private boolean validateMessage(Message message) {
        var isValid = message.getMessageId() != null && message.getMessageId() > 0;
        if (!isValid) {
            logger.warn(
                    "Message {} isn't valid", message);
        }
        return isValid;
    }

}
