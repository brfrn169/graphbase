package com.github.brfrn169.graphbase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.util.Map;

@Data @Accessors(fluent = true) @EqualsAndHashCode(of = {"outNodeId", "type", "inNodeId"})
public class Relationship implements Entity {
    @NonNull @JsonProperty("outNodeId") private final String outNodeId;
    @NonNull @JsonProperty("type") private final String type;
    @NonNull @JsonProperty("inNodeId") private final String inNodeId;
    @NonNull @JsonProperty("properties") private final Map<String, Object> properties;

    @JsonCreator public Relationship(@NonNull @JsonProperty("outNodeId") String outNodeId,
        @JsonProperty("type") String type, @JsonProperty("inNodeId") String inNodeId,
        @JsonProperty("properties") Map<String, Object> properties) {
        this.outNodeId = outNodeId;
        this.type = type;
        this.inNodeId = inNodeId;
        this.properties = properties;
    }
}
