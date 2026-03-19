package ru.sb.demo.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.sb.demo.model.Message;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class MessageServiceImpl implements MessageService {
    private static final Logger logger = LogManager.getLogger(MessageServiceImpl.class);

    private final JdbcTemplate jdbcTemplate;

    public MessageServiceImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void handleMessages(List<Message> messages) {
        String sql = "INSERT INTO message (message_id, payload) VALUES (?, ?) ON CONFLICT (message_id) DO NOTHING";
        
        jdbcTemplate.batchUpdate(sql, new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Message msg = messages.get(i);
                ps.setLong(1, msg.getMessageId());
                ps.setString(2, msg.getPayload());
            }

            @Override
            public int getBatchSize() {
                return messages.size();
            }
        });
    }
}
