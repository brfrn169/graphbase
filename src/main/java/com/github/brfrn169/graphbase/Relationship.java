package com.github.brfrn169.graphbase;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

@Data @EqualsAndHashCode(exclude = "properties") public class Relationship implements Entity {
    private final String outNodeId;
    private final String type;
    private final String inNodeId;
    private final Map<String, Object> properties;
}
