DROP TABLE IF EXISTS message;
CREATE TABLE message
(
    message_id      BIGINT    NOT NULL,
    payload         VARCHAR(100),

    CONSTRAINT pk_message_id PRIMARY KEY (message_id)
);