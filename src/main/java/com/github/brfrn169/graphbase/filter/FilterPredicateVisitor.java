package com.github.brfrn169.graphbase.filter;


public interface FilterPredicateVisitor<T, R> {
    R visit(EqualFilterPredicate filterPredicate, T context);

    R visit(NotEqualFilterPredicate filterPredicate, T context);

    R visit(GreaterFilterPredicate filterPredicate, T context);

    R visit(GreaterOrEqualFilterPredicate filterPredicate, T context);

    R visit(LessFilterPredicate filterPredicate, T context);

    R visit(LessOrEqualFilterPredicate filterPredicate, T context);

    R visit(IsNullFilterPredicate filterPredicate, T context);

    R visit(IsNotNullFilterPredicate filterPredicate, T context);

    R visit(RegexFilterPredicate filterPredicate, T context);

    R visit(CompositeFilterPredicate compositeFilter, T context);
}
