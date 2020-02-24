package ru.sb.demo;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.ListUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.api.Assertions;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.json.AutoConfigureJson;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.orm.jpa.JpaSystemException;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import ru.sb.demo.config.KafkaConfig;
import ru.sb.demo.config.ServiceConfig;
import ru.sb.demo.config.TestKafkaProducerConfig;
import ru.sb.demo.model.Message;
import ru.sb.demo.model.MessageBatch;
import ru.sb.demo.repository.MessageRepository;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.util.stream.StreamSupport.stream;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.Mockito.*;

@SpringBootTest(
        properties = {"app.batchTimeout=3000", "app.batchSize=4", "app.handledMessagePartitions=1",
                "app.incomingMessagePartitions=1","spring.kafka.server=PLAINTEXT://localhost:9094"},
        classes = TestMessageHandlers.TestMessagingConfig.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE)
@AutoConfigureJson
//@ContextConfiguration(initializers = TestMessageHandlers.Initializer.class)
@Testcontainers
public class TestMessageHandlers {

    @Configuration
    @Import({ServiceConfig.class, KafkaConfig.class, TestKafkaProducerConfig.class})
    static class TestMessagingConfig {
    }

    @Container
    private static DockerComposeContainer kafkaContainer = startKafkaContainer();

    @Value("${app.incomingMessageTopic}")
    private String incomingMessageTopic;

    @Value("${app.batchTimeout}")
    private Integer batchTimeout;

    @Value("${app.batchSize}")
    private Integer batchSize;

    @Autowired
    KafkaProducer incomingMsgProducer;

    @MockBean
    private MessageRepository messageRepository;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @BeforeEach
    public void setUp() {
        // waiting for partitions assign
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
                .getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(
                    messageListenerContainer, 1
            );
        }
    }

    @Test
    public void testShouldStoreAllMessagesFromPayload() {
        List<Message> receivedMessages = createRepositoryCapture();

        final var messages = LongStream.range(1, 2)
                .mapToObj(i -> buildMessage(i, "Payload " + i))
                .collect(Collectors.toList());

        sendMessageBatch(Collections.emptyList());
        sendMessageBatch(messages);

        await().atMost(Duration.ofMillis(batchTimeout + 1_000))
                .with()
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() ->
                        Assertions.assertThat(receivedMessages)
                                .containsExactlyInAnyOrderElementsOf(messages)
                );


    }

    @Test
    public void testShouldFilerNotValidMessagesFromPayload() {
        List<Message> receivedMessages = createRepositoryCapture();

        final var validMessages = LongStream.range(1, 2)
                .mapToObj(i -> buildMessage(i, "Payload " + i))
                .collect(Collectors.toList());

        List<Message> messages = new ArrayList<>(validMessages);
        messages.add(buildMessage(0, "Payload 0"));

        sendMessageBatch(messages);
        sendMessageBatch(Collections.emptyList());

        await().atMost(Duration.ofMillis(batchTimeout + 1_000))
                .with()
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() ->
                        Assertions.assertThat(receivedMessages)
                                .containsExactlyInAnyOrderElementsOf(validMessages)
                );


    }

    @Test
    public void testShouldFilerEmptyBatches() {

        sendMessageBatch(Collections.emptyList());
        sendMessageBatch(Collections.emptyList());
        sendMessageBatch(Collections.emptyList());

        Assertions.assertThatExceptionOfType(ConditionTimeoutException.class).isThrownBy(() -> {
            await().atMost(Duration.ofMillis(batchTimeout * 2)).until(() -> {
                verify(messageRepository, times(0)).saveAll(anyIterable());
                return false;
            });
        });
    }

    @Test
    public void testShouldSaveMessagesOfBatchSizeBeforeTimeout() {

        List<Message> receivedMessages = createRepositoryCapture();

        final var messages = LongStream.range(0, batchSize)
                .mapToObj(i -> buildMessage(i + 1, "Payload " + i))
                .collect(Collectors.toList());

        sendMessageBatch(messages);

        await().atMost(Duration.ofSeconds(1))
                .with()
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() ->
                        Assertions.assertThat(receivedMessages)
                                .containsExactlyInAnyOrderElementsOf(messages)
                );
    }

    @Test
    public void testShouldBeSplittedMessagesByBatchSize() {

        List<Message> receivedMessages = createRepositoryCapture();

        final var batch = LongStream.range(0, batchSize / 2)
                .mapToObj(i -> buildMessage(0, "Payload " + i))
                .collect(Collectors.toList());

        final var batch2 = LongStream.range(0, batchSize / 2)
                .mapToObj(i -> buildMessage(i + 1, "Payload " + i))
                .collect(Collectors.toList());

        final var batch3 = LongStream.range(batchSize / 2, batchSize)
                .mapToObj(i -> buildMessage(i + 1, "Payload " + i))
                .collect(Collectors.toList());


        var messages = new ArrayList<>(batch3);
        Message extraMessage = buildMessage(1, "Payload ");
        messages.add(extraMessage);

        sendMessageBatch(batch);
        sendMessageBatch(batch2);
        sendMessageBatch(messages);


        await().atMost(Duration.ofSeconds(1))
                .with()
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() ->
                        Assertions.assertThat(receivedMessages)
                                .containsExactlyInAnyOrderElementsOf(ListUtils.union(batch2, batch3))
                );

        await().atMost(Duration.ofMillis(batchTimeout + 1_000))
                .with()
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() ->
                        Assertions.assertThat(receivedMessages)
                                .containsExactlyInAnyOrderElementsOf(List.of(extraMessage))
                );
    }

    @Test
    public void testErrorHandleOnBatchSave() {

        final var receivedMessages = new CopyOnWriteArrayList<Message>();

        final var expectedMessages = LongStream.range(1, 3)
                .mapToObj(i -> buildMessage(i, "Payload " + i))
                .collect(Collectors.toList());

        doThrow(new JpaSystemException(new RuntimeException()))
                .doAnswer(invocation -> {
                    Iterable<Message> messages = invocation.getArgument(0);
                    var captured = stream(messages.spliterator(), false).map(m ->
                            buildMessage(m.getMessageId(), m.getPayload())
                    ).collect(Collectors.toList());
                    receivedMessages.addAll(captured);
                    return null;
                }).when(messageRepository).saveAll(anyIterable());


        sendMessageBatch(expectedMessages);

        await().atMost(Duration.ofMillis(batchTimeout + 1_000))
                .with()
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    verify(messageRepository, atLeastOnce()).saveAll(expectedMessages);
                });

        doAnswer(invocation -> {
            receivedMessages.clear();
            Iterable<Message> messages = invocation.getArgument(0);
            var captured = stream(messages.spliterator(), false).map(m ->
                    buildMessage(m.getMessageId(), m.getPayload())
            ).collect(Collectors.toList());
            System.out.println("Captured messages: " + captured.toString());
            receivedMessages.addAll(captured);
            return null;
        }).when(messageRepository).saveAll(anyIterable());

        await().atMost(Duration.ofMillis(batchTimeout + 1_000))
                .with()
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() ->
                        Assertions.assertThat(receivedMessages)
                                .containsExactlyInAnyOrderElementsOf(expectedMessages)
                );


    }


    private void sendMessageBatch(List<Message> batch) {
        ObjectMapper mapper = new ObjectMapper();
        var payload = new MessageBatch();
        payload.setMessages(batch);
        try {
            ProducerRecord<Long, String> record = new ProducerRecord<>(incomingMessageTopic,
                    mapper.writeValueAsString(payload));
            incomingMsgProducer.send(record).get();
        } catch (Exception e) {
            Assertions.fail(e.getMessage(), e);
        }

    }

    private List<Message> createRepositoryCapture() {
        var capturedMessages = new CopyOnWriteArrayList<Message>();
        doAnswer(invocation -> {
            capturedMessages.clear();
            Iterable<Message> messages = invocation.getArgument(0);
            var captured = stream(messages.spliterator(), false).map(m ->
                    buildMessage(m.getMessageId(), m.getPayload())
            ).collect(Collectors.toList());
            System.out.println("Captured messages: " + captured.toString());
            capturedMessages.addAll(captured);
            return null;
        }).when(messageRepository).saveAll(anyIterable());
        return capturedMessages;
    }

    private Message buildMessage(long id, String payload) {
        final var message = new Message();
        message.setMessageId(id);
        message.setPayload(payload);
        return message;
    }

    private static DockerComposeContainer startKafkaContainer() {


        DockerComposeContainer composeContainer = new DockerComposeContainer(
                new File(TestMessageHandlers.class.getResource("/docker-compose-kafka.yml").getFile()))
                .withExposedService("kafka_1", 9094,
                        Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60)));




        final var container = new KafkaContainer();
//        container.setExposedPorts(List.of(9093));

        composeContainer.start();


        return composeContainer;
    }

    static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            // System.out.println("+++++++++++++++++++++++++++++++++" + kafkaContainer.getBootstrapServers()+ "+++++++++++++++");
            TestPropertyValues values = TestPropertyValues.of(
                    "spring.kafka.server=PLAINTEXT://localhost:9094"
            );
            values.applyTo(configurableApplicationContext);
        }
    }
}
