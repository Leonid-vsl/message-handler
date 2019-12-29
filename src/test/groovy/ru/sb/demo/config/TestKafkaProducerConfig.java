package ru.sb.demo.config;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import ru.sb.demo.kafka.TestConsumerRebalanceListener;

import java.util.Collection;
import java.util.Properties;

@TestConfiguration()
public class TestKafkaProducerConfig {

    @Value("${spring.kafka.server}")
    private String kafkaServer;

    @Value("${app.incomingMessageTopic}")
    private String incomingMessageTopic;

    @Bean
    public KafkaProducer incomingMessageKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "incoming-message-handler");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        return new KafkaProducer<>(props);
    }

    @Primary
    @Bean
    public TestConsumerRebalanceListener incomingMessageRebalanceListener(){
        return new TestConsumerRebalanceListener();
    }


}
