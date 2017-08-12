package com.github.brfrn169.graphbase.sort;

import lombok.Data;
import lombok.NonNull;

@Data public class SortPredicate {

    public enum Operator {
        ASC, DESC
    }


    public static final class Builder {
        public static SortPredicate asc(String propertyKey) {
            return new SortPredicate(propertyKey, SortPredicate.Operator.ASC);
        }

        public static SortPredicate desc(String propertyKey) {
            return new SortPredicate(propertyKey, SortPredicate.Operator.DESC);
        }
    }


    @NonNull public final String propertyKey;
    @NonNull public final Operator operator;
}
