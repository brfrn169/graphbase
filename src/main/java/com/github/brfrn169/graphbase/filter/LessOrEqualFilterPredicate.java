package com.github.brfrn169.graphbase.filter;

import lombok.Data;
import lombok.NonNull;

@Data public class LessOrEqualFilterPredicate implements FilterPredicate {
    @NonNull public final String propertyKey;
    @NonNull public final Object propertyValue;

    @Override public <T, R> R accept(FilterPredicateVisitor<T, R> visitor, T context) {
        return visitor.visit(this, context);
    }
}
