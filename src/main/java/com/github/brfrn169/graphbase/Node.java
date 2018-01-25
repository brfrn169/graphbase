package com.github.brfrn169.graphbase;


import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.util.Map;

@Data @Accessors(fluent = true) @EqualsAndHashCode(of = "id") public class Node implements Entity {
    @NonNull private final String id;
    @NonNull private final String type;
    @NonNull private final Map<String, Object> properties;
}
