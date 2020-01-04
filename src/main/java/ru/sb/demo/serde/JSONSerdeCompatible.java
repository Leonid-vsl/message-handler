package ru.sb.demo.serde;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import ru.sb.demo.model.Message;
import ru.sb.demo.model.MessageBatch;

@SuppressWarnings("DefaultAnnotationParam") // being explicit for the example
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Message.class, name = "msg"),
        @JsonSubTypes.Type(value = MessageBatch.class, name = "msgBtc")
})
public interface JSONSerdeCompatible {

}