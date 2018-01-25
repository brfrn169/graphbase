package com.github.brfrn169.graphbase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;

@Data @Accessors(fluent = true) public class GraphConfiguration {
    @NonNull @JsonProperty("graphId") private final String graphId;

    @JsonCreator public GraphConfiguration(@NonNull @JsonProperty("graphId") String graphId) {
        this.graphId = graphId;
    }
}
