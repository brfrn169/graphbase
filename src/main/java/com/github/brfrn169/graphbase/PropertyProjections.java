package com.github.brfrn169.graphbase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public final class PropertyProjections {

    public enum Type {
        ALL, NOTHING, PARTIAL
    }


    private final Type type;
    private final Set<String> propertyKeys;

    @JsonCreator private PropertyProjections(@JsonProperty("type") Type type,
        @JsonProperty("propertyKeys") Set<String> propertyKeys) {
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
