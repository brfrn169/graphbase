package com.github.brfrn169.graphbase.util;

import java.util.HashMap;
import java.util.Map;

public final class Properties {
    private Properties() {
    }

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

    public static Map<String, Object> property(String key, Object value) {
        Map<String, Object> ret = new HashMap<>();
        ret.put(key, value);
        return ret;
    }

    public static Map<String, Object> property(String key1, Object value1, String key2,
        Object value2) {
        Map<String, Object> ret = new HashMap<>();
        ret.put(key1, value1);
        ret.put(key2, value2);
        return ret;
    }
}
