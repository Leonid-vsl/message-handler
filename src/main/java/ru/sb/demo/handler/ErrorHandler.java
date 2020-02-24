package ru.sb.demo.handler;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.listener.ContainerAwareBatchErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.Collectors;

public class ErrorHandler implements ContainerAwareBatchErrorHandler {

    private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(ErrorHandler.class));

    private final Set<Class<? extends Exception>> seekToCurrentExceptions;

    public ErrorHandler(Set<Class<? extends Exception>> seekToCurrentExceptions) {
        this.seekToCurrentExceptions = seekToCurrentExceptions;
    }


    @Override
    public void handle(final Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer, MessageListenerContainer container) {

        if (seekToCurrentExceptions.stream().anyMatch(exceptionClazz ->
                exceptionClazz.isAssignableFrom(thrownException.getCause().getClass()
                ))) {
            data.partitions()
                    .stream()
                    .collect(
                            Collectors.toMap(tp -> tp,
                                    tp -> data.records(tp).get(0).offset(), (u, v) -> (long) v, LinkedHashMap::new))
                    .forEach(consumer::seek);
            throw new KafkaException("Seek to current after exception", thrownException);
        } else {

            StringBuilder message = new StringBuilder("Error while processing:\n");
            if (data == null) {
                message.append("null ");
            } else {
                for (ConsumerRecord<?, ?> record : data) {
                    message.append(record).append('\n');
                }
            }

            LOGGER.error(thrownException, () -> message.substring(0, message.length() - 1));
        }

    }
}
