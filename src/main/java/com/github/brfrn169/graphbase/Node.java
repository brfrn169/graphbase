package com.github.brfrn169.graphbase;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.util.Map;

@Data @Accessors(fluent = true) @EqualsAndHashCode(of = "id") public class Node implements Entity {
    @NonNull @JsonProperty("id") private final String id;
    @NonNull @JsonProperty("type") private final String type;
    @NonNull @JsonProperty("properties") private final Map<String, Object> properties;

    @JsonCreator
    public Node(@NonNull @JsonProperty("id") String id, @JsonProperty("type") String type,
        @JsonProperty("properties") Map<String, Object> properties) {
        this.id = id;
        this.type = type;
        this.properties = properties;
    }
}
