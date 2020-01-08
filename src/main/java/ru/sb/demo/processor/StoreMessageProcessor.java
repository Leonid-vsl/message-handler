package ru.sb.demo.processor;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;
import ru.sb.demo.model.Message;
import ru.sb.demo.model.MessageBatch;

@Component
public class StoreMessageProcessor {
    private static final Logger logger = LogManager.getLogger(StoreMessageProcessor.class);

    public void process(KStream<String, MessageBatch> stream) {

        stream.foreach((key, value) -> {

            logger.info("Receive message batch of size: {} ", value.getMessages().size());
        });

    }
}
