package com.github.brfrn169.graphbase;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data public class GraphConfiguration {
    @JsonProperty private final String graphId;
}
