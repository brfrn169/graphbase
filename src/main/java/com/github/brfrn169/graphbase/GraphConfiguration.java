package com.github.brfrn169.graphbase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NonNull;

@Data public class GraphConfiguration {
    @NonNull private final String graphId;

    @JsonCreator public GraphConfiguration(@NonNull @JsonProperty("graphId") String graphId) {
        this.graphId = graphId;
    }
}
