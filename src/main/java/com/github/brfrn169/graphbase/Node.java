package com.github.brfrn169.graphbase;


import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

import java.util.Map;

@Data @EqualsAndHashCode(exclude = {"type", "properties"}) public class Node implements Entity {
    @NonNull private final String id;
    @NonNull private final String type;
    @NonNull private final Map<String, Object> properties;
}
