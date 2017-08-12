package com.github.brfrn169.graphbase;


import java.util.Map;

public interface Entity {
    Map<String, Object> getProperties();

    default Object getPropertyValue(String key) {
        return getProperties().get(key);
    }
}
