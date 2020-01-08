package ru.sb.demo.processor;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.sb.demo.model.Message;
import ru.sb.demo.model.MessageBatch;

import static java.time.Duration.ofMillis;

@Component
public class AggregationMessageProcessor {
    private static final Logger logger = LogManager.getLogger(AggregationMessageProcessor.class);

    @Value("${app.aggregatedMessageTopic}")
    private String aggregatedMessageTopic;

    @Value("${app.batchTimeout}")
    private Long batchTimeout;

    @Autowired
    private Serde<MessageBatch> messageBatchSerde;
    @Autowired
    private Serde<Message> messageSerde;

    public void process(KStream<String, Message> stream) {

        stream.groupByKey(Grouped.with(Serdes.String(), messageSerde))
                .windowedBy(TimeWindows.of(ofMillis(batchTimeout)).advanceBy(ofMillis(1_000)))
                .aggregate(MessageBatch::new, (key, value, aggregate) -> {
                            aggregate.getMessages().add(value);
                            return aggregate;
                        }
                        , Materialized.with(Serdes.String(), messageBatchSerde)
                ).
                toStream().to(aggregatedMessageTopic);

    }
}
