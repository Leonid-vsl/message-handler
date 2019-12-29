package ru.sb.demo.handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.messaging.handler.annotation.Payload;
import ru.sb.demo.model.Message;
import ru.sb.demo.model.MessageBatch;

import java.util.List;

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
        batch.stream()
                .filter(this::filterEmptyBatches)
                .flatMap(payload -> payload.getMessages().stream())
                .filter(this::validateMessage)
                .forEach(message ->
                        handledMessageTemplate.send(
                                handledMessageTopic,
                                message.getMessageId(),
                                message
                        ));

    }

    private boolean filterEmptyBatches(MessageBatch payload) {
        boolean isEmpty = !payload.getMessages().isEmpty();
        if (!isEmpty) {
            logger.warn("Batch is empty");
        }
        return isEmpty;
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
