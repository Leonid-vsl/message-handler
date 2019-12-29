package ru.sb.demo.model;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.List;

public class MessageBatch {

    @NotEmpty(message = "Batch is empty")
    //@Size(min = 1,message = "Size should be greater than 0")
    private List<Message> messages;

    public List<Message> getMessages() {
        return messages;
    }

    public void setMessages(List<Message> messages) {
        this.messages = messages;
    }

    @Override
    public String toString() {
        return "MessageBatch{" +
                "messages=" + messages +
                '}';
    }
}
