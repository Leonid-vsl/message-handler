package ru.sb.demo.processor;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.sb.demo.model.Message;
import ru.sb.demo.model.MessageBatch;

@Component
public class StoreMessageProcessor {
    private static final Logger logger = LogManager.getLogger(StoreMessageProcessor.class);

    @Autowired
    private Serde<MessageBatch> messageBatchSerde;

    public void process(KStream<Windowed<String>, Message> stream) {

        stream.groupByKey().aggregate(MessageBatch::new, (key, value, aggregate) -> {
            aggregate.getMessages().add(value);
            return aggregate;
        }, Materialized.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), messageBatchSerde)).toStream().
        foreach((key, value) -> {

            logger.info("Receive message batch of size: {} ", value.getMessages().size());
            logger.info("Message batch: {} ", value);
        });

    }
}
