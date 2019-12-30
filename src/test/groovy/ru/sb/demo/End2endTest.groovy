package ru.sb.demo

import groovy.json.JsonOutput
import groovy.sql.Sql
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.testcontainers.containers.DockerComposeContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.spock.Testcontainers
import ru.sb.demo.config.TestKafkaProducerConfig
import ru.sb.demo.kafka.TestConsumerRebalanceListener
import ru.sb.demo.model.Message
import ru.sb.demo.model.MessageBatch
import ru.sb.demo.repository.MessageRepository
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll
import spock.util.concurrent.PollingConditions


import java.time.Duration

@ActiveProfiles("test")
@SpringBootTest(
        value = "spring.main.allow-bean-definition-overriding=true",
        classes = [Application, TestKafkaProducerConfig],
        webEnvironment = SpringBootTest.WebEnvironment.NONE
)
@Testcontainers
class End2endTest extends Specification {

    @Shared
    DockerComposeContainer composeContainer = new DockerComposeContainer(
            new File("docker-compose.yml"))
            .withExposedService("pgsql_1", 5432,
                    Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(30)))
            .withExposedService("kafka_1", 9092,
                    Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(30)))


    @Autowired
    KafkaProducer incomingMsgProducer

    @Autowired
    @Qualifier('incomingMessageRebalanceListener')
    TestConsumerRebalanceListener partitionListener

    @Autowired
    MessageRepository repository

    @Value('${app.incomingMessageTopic}')
    private String incomingMessageTopic

    @Shared
    def sql

    def setupSpec() {

        sql = Sql.newInstance("jdbc:postgresql://0.0.0.0:5432/sb_demo", "demo", "demo", "org.postgresql.Driver")
        sql.call(new File(getClass().getResource("/db/migration/V1__schema_creation.sql").getPath()).text)


    }

    def setup() {
        PollingConditions initCondition = new PollingConditions(timeout: 30)
        initCondition.eventually {
            partitionListener.isAssigned()
        }
    }

    def cleanup() {
        sql.call('DELETE FROM message')
        assert repository.findAll() == []
    }

    @Shared
    def msg0 = new Message(messageId: 0, payload: '0')
    @Shared
    def msg1 = new Message(messageId: 1, payload: '1')
    @Shared
    def msg2 = new Message(messageId: 2, payload: '2')
    @Shared
    def msg3 = new Message(messageId: 3, payload: '3')
    @Shared
    def msg4 = new Message(messageId: 4, payload: '4')
    @Shared
    def msg5 = new Message(messageId: 5, payload: '5')
    @Shared
    def msg6 = new Message(messageId: 6, payload: '6')

    @Unroll
    def "test for multiple batch handling"() {

        setup:
        PollingConditions testCondition = new PollingConditions(timeout: 30)

        when:
        incomingMessage
                .forEach({ msg ->
                    ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(incomingMessageTopic,
                            JsonOutput.toJson(msg));
                    incomingMsgProducer.send(record).get()
                });

        then:

        testCondition.eventually {
            assert repository.findAll() as Set == stored.sort() as Set
        }

        where:
        incomingMessage                                                                      | stored
        [new MessageBatch(messages: [])]                                                     | []
        [new MessageBatch(messages: [msg1]), new MessageBatch(messages: [msg1])]             | [msg1]
        [new MessageBatch(messages: [msg1])]                                                 | [msg1]
        [new MessageBatch(messages: [msg1, msg1])]                                           | [msg1]
        [new MessageBatch(messages: [msg1, msg2])]                                           | [msg1, msg2]
        [new MessageBatch(messages: [msg1, msg2]), new MessageBatch(messages: [msg3, msg4])] | [msg1, msg2, msg3, msg4]
        [new MessageBatch(messages: [msg1, msg2, msg3, msg4, msg5, msg0])]                   | [msg1, msg2, msg3, msg4, msg5]

    }

}
