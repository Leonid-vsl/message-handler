package ru.sb.demo.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.orm.jpa.JpaSystemException;
import ru.sb.demo.model.Message;
import ru.sb.demo.repository.MessageRepository;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.util.Collection;

public class MessageServiceImpl implements MessageService {
    private static final Logger logger = LogManager.getLogger(MessageServiceImpl.class);

    private final MessageRepository messageRepository;

    public MessageServiceImpl(MessageRepository messageRepository) {
        this.messageRepository = messageRepository;
    }

    @Override
    public void handleMessages(Collection<Message> messages) {

        try {
            messageRepository.saveAll(messages);
        } catch (JpaSystemException e) {
            logger.error("Connection exception", e);
            throw new DataBaseNotAvailable();
        } catch (DataIntegrityViolationException e) {
            BatchUpdateException batchUpdateException = (BatchUpdateException) e.getCause().getCause();
            SQLException item = batchUpdateException.getNextException();
            while (item != null) {
                logger.error(item.getMessage());
                item = item.getNextException();
            }
        } catch (Exception e) {
            logger.error("Error while saving data", e);
        }
    }
}
