package ru.sb.demo.processor;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.sb.demo.model.Message;
import ru.sb.demo.model.MessageBatch;

import java.util.stream.Collectors;

@Component
public class IncomingMessageProcessor {

    private static final Logger logger = LogManager.getLogger(IncomingMessageProcessor.class);

    @Value("${app.handledMessageTopic}")
    private String handledMessageTopic;

    @Autowired
    private Serde<Message> messageSerde;

    @Autowired
    private BatchKeyGen batchKeyGen;


    public void process(KStream<Long, MessageBatch> stream) {
        stream
                .peek((key, value) -> {
                    logger.info("Handling incoming message: {}", value);
                })
                .filter((key, value) -> {
                    var isNotEmpty = value.getMessages() != null && value.getMessages().size() > 0;
                    if (!isNotEmpty) {
                        logger.info("Incoming batch message is empty. Msg: {}", value);
                    }
                    return isNotEmpty;
                })
                .flatMap((key, value) ->
                        value.getMessages().stream().map(message ->
                                new KeyValue<>((long) message.getMessageId(), message)
                        ).collect(Collectors.toList())
                ).filter((key, value) -> {
                    var isNotZero = value.getMessageId() > 0;
                    if (!isNotZero) {
                        logger.info("Incoming message id isn't valid. Msg: {}", value);
                    }
                    return isNotZero;
                }).map((key, value) ->
                        new KeyValue<>(batchKeyGen.generateId(), value)
                 )
               .to(handledMessageTopic, Produced.with(Serdes.String(), messageSerde));
    }
}
