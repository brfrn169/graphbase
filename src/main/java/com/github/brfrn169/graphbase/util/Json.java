package com.github.brfrn169.graphbase.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.brfrn169.graphbase.exception.GraphbaseException;
import com.xebia.jacksonlombok.JacksonLombokAnnotationIntrospector;

import java.io.IOException;


public final class Json {

    private final ObjectMapper objectMapper;

    public Json(JsonInclude.Include include) {
        objectMapper =
            new ObjectMapper().setAnnotationIntrospector(new JacksonLombokAnnotationIntrospector())
                .setSerializationInclusion(include);
    }

    public String writeValueAsString(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new GraphbaseException("an error occurred during json processing.", e);
        }
    }

    public byte[] writeValueAsBytes(Object value) {
        try {
            return objectMapper.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new GraphbaseException("an error occurred during json processing.", e);
        }
    }

    public <T> T readValue(String content, Class<T> valueType) {
        try {
            return objectMapper.readValue(content, valueType);
        } catch (IOException e) {
            throw new GraphbaseException("an error occurred during json processing.", e);
        }
    }

    public <T> T readValue(byte[] src, Class<T> valueType) {
        try {
            return objectMapper.readValue(src, valueType);
        } catch (IOException e) {
            throw new GraphbaseException("an error occurred during json processing.", e);
        }
    }
}
