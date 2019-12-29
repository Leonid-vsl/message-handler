package ru.sb.demo.service;

import ru.sb.demo.model.Message;

import java.util.Collection;
import java.util.List;

public interface MessageService
{
    void handleMessages(List<Message> messages);
}
