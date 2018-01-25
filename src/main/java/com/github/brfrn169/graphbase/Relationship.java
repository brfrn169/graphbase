package com.github.brfrn169.graphbase;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.util.Map;

@Data @Accessors(fluent = true) @EqualsAndHashCode(of = {"outNodeId", "type", "inNodeId"})
public class Relationship implements Entity {
    @NonNull private final String outNodeId;
    @NonNull private final String type;
    @NonNull private final String inNodeId;
    @NonNull private final Map<String, Object> properties;
}
