package ru.sb.demo.processor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class BatchKeyGen {
    @Value("${app.batchSize}")
    private Integer batchSize;

    private final AtomicLong counter;

    private final AtomicLong batchCounter;

    private final String format = "%.8s-%d";

    private final String uuid;

    public BatchKeyGen() {
        this.counter = new AtomicLong();
        this.batchCounter = new AtomicLong();
        this.uuid = UUID.randomUUID().toString();

    }

    public String generateId() {
        long batchCnt = batchCounter.get();

        if (batchCnt == batchSize) {
            if (batchCounter.compareAndSet(batchCnt, 0)) {
                counter.incrementAndGet();
            }
        }

        batchCounter.incrementAndGet();
        return String.format(format, uuid, counter.get());
    }
}
