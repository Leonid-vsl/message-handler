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
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.DockerComposeContainer;
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
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.util.List.of;
import static java.util.stream.StreamSupport.stream;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.Mockito.*;

/**
 * TODO
 * <p>
 * * Test for offset after restart
 * * Partition re-assignment
 */


@SpringBootTest(
        properties = {"app.batchTimeout=3000", "app.batchSize=4", "app.handledMessagePartitions=1",
                "app.incomingMessagePartitions=1", "spring.kafka.server=PLAINTEXT://localhost:9094"},
        classes = TestMessageHandlers.TestMessagingConfig.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE)
@AutoConfigureJson
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
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

    private AtomicLong idProvider = new AtomicLong();

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

        final var messages = produceMessages(2);

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

        final var validMessages = produceMessages(2);

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

        final var messages = produceMessages(batchSize);

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

        final var firstHalfOfBatch = produceMessages(batchSize / 2);
        final var secondHalfOfBatch = produceMessages(batchSize / 2);

        Message extraMessage = buildMessage(1, "Payload ");

        sendMessageBatch(Collections.emptyList());
        sendMessageBatch(firstHalfOfBatch);
        sendMessageBatch(Collections.emptyList());
        sendMessageBatch(secondHalfOfBatch);
        sendMessageBatch(Collections.emptyList());
        sendMessageBatch(of(extraMessage));


        await().atMost(Duration.ofSeconds(1))
                .with()
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() ->
                        Assertions.assertThat(receivedMessages)
                                .containsExactlyInAnyOrderElementsOf(ListUtils.union(firstHalfOfBatch, secondHalfOfBatch))
                );

        await().atMost(Duration.ofMillis(batchTimeout + 1_000))
                .with()
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() ->
                        Assertions.assertThat(receivedMessages)
                                .containsExactlyInAnyOrderElementsOf(of(extraMessage))
                );
    }

    @Test
    public void testErrorHandlerShouldReStoreMessagesCausingSeekableExceptions() {

        final var expectedMessages = produceMessages(2);

        doThrow(new JpaSystemException(new RuntimeException())).when(messageRepository).saveAll(expectedMessages);

        sendMessageBatch(expectedMessages);

        await().atMost(Duration.ofMillis(batchTimeout + 1_000))
                .with()
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    verify(messageRepository, atLeastOnce()).saveAll(expectedMessages);
                });

        final var afterThrowMessages = createRepositoryCapture();

        await().atMost(Duration.ofMillis(batchTimeout + 1_000))
                .with()
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() ->
                        Assertions.assertThat(afterThrowMessages)
                                .containsExactlyInAnyOrderElementsOf(expectedMessages)
                );


    }

    @Test
    public void testErrorHandleShouldSkipMessagesCausingNonSeekableExceptions() {

        final var batch1 = produceMessages(2);

        doThrow(new RuntimeException()).when(messageRepository).saveAll(batch1);

        sendMessageBatch(batch1);

        await().atMost(Duration.ofMillis(batchTimeout + 1_000))
                .with()
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    verify(messageRepository, times(1)).saveAll(batch1);
                });

        List<Message> receivedAfterThrow = createRepositoryCapture();

        final var batch2 = produceMessages(2);

        sendMessageBatch(batch2);

        await().atMost(Duration.ofMillis(batchTimeout + 1_000))
                .with()
                .pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() ->
                        Assertions.assertThat(batch2)
                                .containsExactlyInAnyOrderElementsOf(receivedAfterThrow)
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

    private List<Message> produceMessages(int amount) {
        return LongStream.range(1, amount + 1)
                .mapToObj(i -> buildMessage(idProvider.incrementAndGet(), "Payload " + UUID.randomUUID().toString()))
                .collect(Collectors.toList());
    }

    private static DockerComposeContainer startKafkaContainer() {


        DockerComposeContainer composeContainer = new DockerComposeContainer(
                new File(TestMessageHandlers.class.getResource("/docker-compose-kafka.yml").getFile()))
                .withExposedService("kafka_1", 9094,
                        Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60)));

        composeContainer.start();

        return composeContainer;
    }

}
