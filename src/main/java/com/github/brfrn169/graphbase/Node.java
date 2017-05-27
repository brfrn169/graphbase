package com.github.brfrn169.graphbase;


import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

@Data @EqualsAndHashCode(exclude = {"type", "properties"}) public class Node implements Entity {
    private final String id;
    private final String type;
    private final Map<String, Object> properties;
}
