package ru.sb.demo

import groovy.json.JsonOutput
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.BeforeEach
import org.spockframework.spring.SpringBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.autoconfigure.json.AutoConfigureJson
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.listener.MessageListenerContainer
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.ContainerTestUtils
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import ru.sb.demo.config.KafkaConfig
import ru.sb.demo.config.TestKafkaProducerConfig
import ru.sb.demo.model.Message
import ru.sb.demo.model.MessageBatch
import ru.sb.demo.service.DataBaseNotAvailable
import ru.sb.demo.service.MessageService
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll
import spock.util.concurrent.PollingConditions

@SpringBootTest(classes = [KafkaConfig, TestKafkaProducerConfig])
@AutoConfigureJson
@EnableKafka
@EmbeddedKafka(ports = 9092, topics = ['${app.handledMessageTopic}', '${app.incomingMessageTopic}'], partitions = 1)
class TestBDConnectionErrorHandle extends Specification {

    @SpringBean
    MessageService messageService = Mock()

    @Autowired
    private EmbeddedKafkaBroker kafkaEmbedded;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry

    @Autowired
    KafkaProducer incomingMsgProducer

    @Value('${app.incomingMessageTopic}')
    private String incomingMessageTopic

    @Shared
    def msg1 = new Message(messageId: 1, payload: '1')

    @BeforeEach
    def setup() {
        // wait until the partitions are assigned
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
                .getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(
                    messageListenerContainer, kafkaEmbedded.getPartitionsPerTopic()
            );
        }
    }


    def "test BD not available handling"() {

        setup:
        PollingConditions testCondition = new PollingConditions(timeout: 30)

        messageService.handleMessages([msg1]) >> {
            throw new DataBaseNotAvailable()
        }


        when:
        incomingMessage
                .forEach({ msg ->
                    ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(incomingMessageTopic,
                            JsonOutput.toJson(msg))
                    incomingMsgProducer.send(record).get()
                });

        then:

        testCondition.eventually {

            1 * messageService.handleMessages(_)
        }




        when:
        messageService.handleMessages([msg1]) >> {}

        then:
        testCondition.eventually {

            messageService.handleMessages([])
        }

        where:
        incomingMessage                                                          | stored
        [new MessageBatch(messages: [msg1]), new MessageBatch(messages: [msg1])] | [msg1]
    }

}
