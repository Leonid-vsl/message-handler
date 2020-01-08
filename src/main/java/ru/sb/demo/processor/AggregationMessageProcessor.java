package ru.sb.demo.processor;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.sb.demo.model.Message;
import ru.sb.demo.model.MessageBatch;

import java.security.Key;

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

    @Autowired
    private BatchKeyGen batchKeyGen;

    public void process(KStream<Long, Message> stream) {

        stream.groupByKey(Grouped.with(Serdes.Long(), messageSerde))
                .windowedBy(TimeWindows.of(ofMillis(batchTimeout)))
                //.windowedBy(SessionWindows.with(ofMillis(batchTimeout)))
                .reduce((a, b) -> {
                    return a;
                }, Materialized.with(Serdes.Long(), messageSerde))
                .toStream()
                .map((key, value) -> {
                    Windowed<String> windowKey = new Windowed<>(batchKeyGen.generateId(), key.window());
                    return new KeyValue<>(windowKey, value);
                })
//                .peek((key, value) -> {
//                    logger.info("aggregated key {} value {}", key, value);
//                })
                .to(aggregatedMessageTopic, Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), messageSerde));
//
//                .peek((key, value) -> {
//                    logger.info("aggregated {}", value);
//                })
//                .to(aggregatedMessageTopic, Produced.with(Serdes.String(), messageBatchSerde));

//                .toStream((key, value) -> {
//                    return new KeyValue<>(batchKeyGen.generateId(), key);
//                })
//                .to(aggregatedMessageTopic, Produced.with(Serdes.String(), messageSerde));

    }
}
