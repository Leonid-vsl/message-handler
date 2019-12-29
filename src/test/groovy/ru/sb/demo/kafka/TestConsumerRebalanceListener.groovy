package ru.sb.demo.kafka

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

class TestConsumerRebalanceListener implements ConsumerRebalanceListener{

    private volatile boolean assigned;

    @Override
    void onPartitionsRevoked(Collection<TopicPartition> partitions) {

    }

    @Override
    void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        assigned = true;
    }

    boolean isAssigned() {
        return assigned
    }
}
