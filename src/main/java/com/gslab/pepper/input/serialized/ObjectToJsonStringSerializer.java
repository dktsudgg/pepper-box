package com.gslab.pepper.input.serialized;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ObjectToJsonStringSerializer implements Serializer<Object> {
    private String encoding = "UTF8";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public ObjectToJsonStringSerializer() {
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null) {
            encodingValue = configs.get("serializer.encoding");
        }

        if (encodingValue instanceof String) {
            this.encoding = (String) encodingValue;
        }

    }

    @Override
    public byte[] serialize(String topic, Object data) {
        try {
            return data == null ? null : objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException var4) {
            throw new SerializationException("Error when serializing object to json str");
        }
    }

}
