package com.github.brfrn169.graphbase;


import java.util.Map;

public interface Entity {
    Map<String, Object> properties();

    default Object propertyValue(String key) {
        return properties().get(key);
    }
}
