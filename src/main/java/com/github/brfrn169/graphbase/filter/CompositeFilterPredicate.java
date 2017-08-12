package com.github.brfrn169.graphbase.filter;

import lombok.Data;
import lombok.NonNull;


@Data public class CompositeFilterPredicate implements FilterPredicate {

    public enum Operator {
        OR, AND
    }


    @NonNull public final FilterPredicate leftFilter;
    @NonNull public final Operator operator;
    @NonNull public final FilterPredicate rightFilter;

    @Override public <T, R> R accept(FilterPredicateVisitor<T, R> visitor, T context) {
        return visitor.visit(this, context);
    }
}
