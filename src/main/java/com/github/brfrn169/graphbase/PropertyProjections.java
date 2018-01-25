package com.github.brfrn169.graphbase;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class PropertyProjections {

    public enum Type {
        ALL, NOTHING, PARTIAL
    }


    private final Type type;
    private final Set<String> propertyKeys;

    private PropertyProjections(Type type, Set<String> propertyKeys) {
        this.type = type;
        this.propertyKeys = propertyKeys;
    }

    public Type getType() {
        return type;
    }

    public Set<String> getPropertyKeys() {
        Preconditions.checkState(type == Type.PARTIAL);
        return propertyKeys;
    }

    public PropertyProjections merge(Set<String> propertyKeys) {
        switch (type) {
            case ALL:
                return this;
            case NOTHING:
                return Builder.withProperties(propertyKeys);
            case PARTIAL:
                return Builder.withProperties(Sets.union(this.propertyKeys, propertyKeys));
            default:
                throw new AssertionError();
        }
    }

    public Map<String, Object> filter(Map<String, Object> properties) {
        switch (type) {
            case ALL:
                return properties;
            case NOTHING:
                if (properties.isEmpty()) {
                    return properties;
                }
                return Collections.emptyMap();
            case PARTIAL:
                if (properties.size() == propertyKeys.size() && propertyKeys.stream()
                    .allMatch(properties::containsKey)) {
                    return properties;
                }

                Map<String, Object> ret = new HashMap<>();
                propertyKeys.forEach(key -> {
                    Object value = properties.get(key);
                    if (value != null) {
                        ret.put(key, value);
                    }
                });
                return ret;
            default:
                throw new AssertionError();
        }
    }

    public static final class Builder {

        private Builder() {
        }

        public static PropertyProjections withProperties(String... keys) {
            Set<String> propKeys = new HashSet<>(Arrays.asList(keys));
            return withProperties(propKeys);
        }

        public static PropertyProjections withProperties(Set<String> keys) {
            if (keys.size() == 0) {
                return withoutProperties();
            }
            return new PropertyProjections(Type.PARTIAL, Collections.unmodifiableSet(keys));
        }

        public static PropertyProjections withoutProperties() {
            return new PropertyProjections(Type.NOTHING, null);
        }

        public static PropertyProjections withAllProperties() {
            return new PropertyProjections(Type.ALL, null);
        }
    }
}
