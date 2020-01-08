package ru.sb.demo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;
import ru.sb.demo.model.Message;
import ru.sb.demo.model.MessageBatch;
import ru.sb.demo.processor.AggregationMessageProcessor;
import ru.sb.demo.processor.IncomingMessageProcessor;
import ru.sb.demo.processor.StoreMessageProcessor;
import ru.sb.demo.serde.JsonPOJODeserializer;
import ru.sb.demo.serde.JsonPOJOSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaConfig {

    @Value("${spring.kafka.server}")
    private String kafkaServer;

    @Value("${app.handledMessageTopic}")
    private String handledMessageTopic;

    @Value("${app.incomingMessageTopic}")
    private String incomingMessageTopic;

    @Value("${app.incomingMessagePartitions}")
    private int incomingMessagePartitions;

    @Value("${app.handledMessagePartitions}")
    private int handledMessagePartitions;

    @Value("${app.aggregatedMessageTopic}")
    private String aggregatedMessageTopic;

    @Value("${app.aggregatedMessagePartitions}")
    private int aggregatedMessagePartitions;

    @Bean
    public NewTopic handledMessageTopic() {
        return new NewTopic(handledMessageTopic, handledMessagePartitions, (short) 1);
    }

    @Bean
    public NewTopic incomingMessageTopic() {
        return TopicBuilder.name(incomingMessageTopic)
                .partitions(incomingMessagePartitions)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic aggregatedMessageTopic() {
        return TopicBuilder.name(aggregatedMessageTopic)
                .partitions(aggregatedMessagePartitions)
                .replicas(1)
                .build();
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "messageHandler");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);

        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                RecoveringDeserializationExceptionHandler.class);

        return new KafkaStreamsConfiguration(props);
    }


    @Bean
    public Serde<MessageBatch> createBatchSerde() {

        JsonPOJOSerializer<MessageBatch> serializer = new JsonPOJOSerializer<>();
        serializer.configure(Collections.singletonMap("JsonPOJOClass", MessageBatch.class), false);

        JsonPOJODeserializer<MessageBatch> deserializer = new JsonPOJODeserializer<>();
        deserializer.configure(Collections.singletonMap("JsonPOJOClass", MessageBatch.class), false);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    @Bean
    public Serde<Message> createMessageSerde() {

        var serializer = new JsonPOJOSerializer<Message>();
        serializer.configure(Collections.singletonMap("JsonPOJOClass", Message.class), false);

        var deserializer = new JsonPOJODeserializer<Message>();
        deserializer.configure(Collections.singletonMap("JsonPOJOClass", Message.class), false);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    @Autowired
    private IncomingMessageProcessor incomingMessageProcessor;

    @Autowired
    private AggregationMessageProcessor persistMessageProcessor;

    @Autowired
    private StoreMessageProcessor storeMessageProcessor;

    @Bean
    public KStream<?, ?> incomingMessageStream(StreamsBuilder kStreamBuilder) {

        var stream = kStreamBuilder.stream(incomingMessageTopic, Consumed.with(Serdes.Long(), createBatchSerde()));
        incomingMessageProcessor.process(stream);
        stream.print(Printed.toSysOut());

        return stream;
    }

    @Bean
    public KStream<?, ?> filteredMessageStream(StreamsBuilder kStreamBuilder) {
        var stream = kStreamBuilder.stream(handledMessageTopic, Consumed.with(Serdes.String(), createMessageSerde()));

        persistMessageProcessor.process(stream);
        stream.print(Printed.toSysOut());
        return stream;
    }

    @Bean
    public KStream<?, ?> storeMessageStream(StreamsBuilder kStreamBuilder) {
        var stream = kStreamBuilder.stream(aggregatedMessageTopic, Consumed.with(Serdes.String(), createBatchSerde()));

        storeMessageProcessor.process(stream);
        stream.print(Printed.toSysOut());
        return stream;
    }

}
