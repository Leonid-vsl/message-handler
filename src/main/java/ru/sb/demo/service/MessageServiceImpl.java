package ru.sb.demo.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.orm.jpa.JpaSystemException;
import ru.sb.demo.model.Message;
import ru.sb.demo.repository.MessageRepository;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;

public class MessageServiceImpl implements MessageService {
    private static final Logger logger = LogManager.getLogger(MessageServiceImpl.class);

    private final MessageRepository messageRepository;

    public MessageServiceImpl(MessageRepository messageRepository) {
        this.messageRepository = messageRepository;
    }

    @Override
    public void handleMessages(List<Message> messages) {

        try {
            messageRepository.saveAll(messages);
        } catch (JpaSystemException e) {
            logger.error("Connection exception", e);
            throw new DataBaseNotAvailable();
        } catch (DataIntegrityViolationException e) {
            BatchUpdateException batchUpdateException = (BatchUpdateException) e.getCause().getCause();
            int[] updateCounts = batchUpdateException.getUpdateCounts();
            for (int i = 0; i < updateCounts.length; i++) {
                if (updateCounts[i] == Statement.EXECUTE_FAILED) {
                    logger.error("Failed to save message {}", messages.get(i));
                }
            }
        } catch (Exception e) {
            logger.error("Error while saving data", e);
        }
    }
}
