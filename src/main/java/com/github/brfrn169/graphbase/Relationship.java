package com.github.brfrn169.graphbase;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

import java.util.Map;

@Data @EqualsAndHashCode(exclude = "properties") public class Relationship implements Entity {
    @NonNull private final String outNodeId;
    @NonNull private final String type;
    @NonNull private final String inNodeId;
    @NonNull private final Map<String, Object> properties;
}
