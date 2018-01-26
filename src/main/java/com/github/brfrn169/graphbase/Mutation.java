package com.github.brfrn169.graphbase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Map;
import java.util.Set;

@Data @Accessors(fluent = true) public class Mutation {
    private final Map<String, Object> setProperties;
    private final Set<String> deleteKeys;

    @JsonCreator public Mutation(@JsonProperty("setProperties") Map<String, Object> setProperties,
        @JsonProperty("deleteKeys") Set<String> deleteKeys) {
        this.setProperties = setProperties;
        this.deleteKeys = deleteKeys;
    }
}
