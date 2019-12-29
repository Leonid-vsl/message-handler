package ru.sb.demo.service;

import ru.sb.demo.model.Message;

import java.util.Collection;

public interface MessageService
{
    void handleMessages(Collection<Message> messages);
}
