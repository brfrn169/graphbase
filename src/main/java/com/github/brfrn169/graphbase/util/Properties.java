package com.github.brfrn169.graphbase.util;

import com.github.brfrn169.graphbase.GraphbaseConstants;
import lombok.experimental.UtilityClass;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

@UtilityClass public class Properties {

    @SuppressWarnings("unchecked")
    public static int comparePropertyValue(Object left, Object right) {
        if (left == null && right == null) {
            return 0;
        } else if (left != null && right == null) {
            return 1;
        } else if (left == null) {
            return -1;
        }

        if (left instanceof Long && right instanceof Integer) {
            return ((Long) left).compareTo((long) (int) right);
        } else if (left instanceof Integer && right instanceof Long) {
            return Long.valueOf((int) left).compareTo((Long) right);
        } else if (left instanceof Double && right instanceof Float) {
            return ((Double) left).compareTo((double) (float) right);
        } else if (left instanceof Float && right instanceof Double) {
            return Double.valueOf((float) left).compareTo((Double) right);
        } else {
            return ((Comparable<Object>) left).compareTo(right);
        }
    }

    public static boolean equals(Map<String, Object> left, Map<String, Object> right) {
        return equals(left, right, false);
    }

    public static boolean equals(Map<String, Object> left, Map<String, Object> right,
        boolean excludeAddAt) {
        if (!excludeAddAt) {
            return left.size() == right.size() && left.entrySet().stream()
                .allMatch(entry -> entry.getValue().equals(right.get(entry.getKey())));
        } else {
            Predicate<Map.Entry<String, Object>> excludePredicate =
                entry -> !entry.getKey().equals(GraphbaseConstants.PROPERTY_ADD_AT);
            long leftCount = left.entrySet().stream().filter(excludePredicate).count();
            long rightCount = right.entrySet().stream().filter(excludePredicate).count();
            return leftCount == rightCount && left.entrySet().stream().filter(excludePredicate)
                .allMatch(entry -> entry.getValue().equals(right.get(entry.getKey())));
        }
    }

    public static Map<String, Object> property(String key, Object value) {
        Map<String, Object> ret = new HashMap<>();
        ret.put(key, value);
        return ret;
    }

    public static Map<String, Object> property(String key1, Object value1, String key2,
        Object value2) {
        Map<String, Object> ret = property(key1, value1);
        ret.put(key2, value2);
        return ret;
    }

    public static Map<String, Object> property(String key1, Object value1, String key2,
        Object value2, String key3, Object value3) {
        Map<String, Object> ret = property(key1, value1, key2, value2);
        ret.put(key3, value3);
        return ret;
    }
}
