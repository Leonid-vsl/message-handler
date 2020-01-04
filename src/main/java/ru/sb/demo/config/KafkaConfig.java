package ru.sb.demo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;
import ru.sb.demo.handler.IncomingMessageHandler;
import ru.sb.demo.handler.MessageStoreHandler;
import ru.sb.demo.model.Message;
import ru.sb.demo.model.MessageBatch;
import ru.sb.demo.service.MessageService;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

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
        // return new NewTopic(incomingMessageTopic, incomingMessagePartitions, (short) 1);
    }

    @Bean
    public Map<String, Object> consumerProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer2.class);
        props.put(ErrorHandlingDeserializer2.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        return props;
    }
//
//    @Bean
//    public Map<String, Object> producerConfigs() {
//        Map<String, Object> props = new HashMap<>();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
//
//        return props;
//    }

//    @Bean
//    public ProducerFactory<Long, Message> producerFactory() {
//        final DefaultKafkaProducerFactory<Long, Message> factory =
//                new DefaultKafkaProducerFactory<>(producerConfigs());
//        return factory;
    // }

//    @Bean
//    public KafkaTemplate<Long, Message> messageKafkaTemplate() {
//        return new KafkaTemplate<>(producerFactory());
//    }

//    @Bean
//    public IncomingMessageHandler incomingMessageHandler(KafkaTemplate<Long, Message> messageKafkaTemplate) {
//        return new IncomingMessageHandler(handledMessageTopic, messageKafkaTemplate);
    // }

//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<Long, MessageBatch> incomingMessageContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<Long, MessageBatch> factory =
//                new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConcurrency(incomingMessagePartitions);
//
//        factory.setConsumerFactory(incomingConsumerFactory());
//        factory.setBatchListener(true);
//
//        return factory;
//    }


//    @Bean
//    public ConsumerFactory<Object, Object> incomingConsumerFactory() {
//        return new DefaultKafkaConsumerFactory<>(incomingMessagesConsumerConfigs());
//    }
//
//    @Bean
//    public Map<String, Object> incomingMessagesConsumerConfigs() {
//        Map<String, Object> props = new HashMap<>(consumerProperties());
//        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, MessageBatch.class);
//        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
//
//
//        return props;
//    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "messageHandler");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // Set a default key serde
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, new JsonSerde<>(MessageBatch.class));

        // props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    @Primary
    public StreamsBuilderFactoryBean myKStreamBuilder(KafkaStreamsConfiguration streamsConfig) {
        return new StreamsBuilderFactoryBean(streamsConfig);
    }


    @Bean
    public KStream<?, ?> kStream(StreamsBuilder kStreamBuilder) {
        KStream<String, String> stream = kStreamBuilder.stream(incomingMessageTopic
                // , Consumed.with(Serdes.Integer(),new JsonSerde<>(MessageBatch.class))
                , Consumed.with(Serdes.String(), Serdes.String())
        );
        stream
                .peek((key, value) -> {
                    System.out.println("111111");
                })

//                .filter((key, batch) -> {
//                    var isEmpty = batch.getMessages().size() > 0;
//                    if (isEmpty) {
//                        System.out.println("Batch is empty");
//                    }
//                    return isEmpty;
//                }).flatMap((key, value) -> value.getMessages().stream().map(message -> new KeyValue<>(message.getMessageId(), message)).collect(toList()))
//                .filterNot((key, message) -> {
//                    var isZero = message.getMessageId() > 0;
//                    if (isZero) {
//                        System.out.println("key is zero");
//                    }
//                    return isZero;
//                })
////                .aggregate(() -> {return new List<Message>()},(key, value, aggregate) -> {
////
////                })
                .to(handledMessageTopic);

        // stream.print();
        //  stream.agg
        stream.print(Printed.toSysOut());
        return stream;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Long, Message> messageStoreContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Long, Message> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(messageStoreConsumerFactory());
        factory.setConcurrency(handledMessagePartitions);
        // factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        factory.setBatchListener(true);
        SeekToCurrentBatchErrorHandler errorHandler = new SeekToCurrentBatchErrorHandler();
        //    factory.getContainerProperties().setIdleBetweenPolls(15_000);
        factory.setBatchErrorHandler(errorHandler);

        return factory;
    }

    @Bean
    @Primary
    public ConsumerFactory<Object, Object> messageStoreConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(messageStoreConsumerConfigs());
    }

    @Bean
    public Map<String, Object> messageStoreConsumerConfigs() {
        Map<String, Object> props = new HashMap<>(consumerProperties());
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Message.class);
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return props;
    }

    @Bean
    public MessageStoreHandler messageStoreHandler(MessageService messageService) {
        return new MessageStoreHandler(messageService);
    }

}
