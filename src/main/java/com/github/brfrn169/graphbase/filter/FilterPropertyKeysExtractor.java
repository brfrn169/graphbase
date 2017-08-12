package com.github.brfrn169.graphbase.filter;

import java.util.HashSet;
import java.util.Set;

public class FilterPropertyKeysExtractor implements FilterPredicateVisitor<Set<String>, Void> {

    private FilterPredicate filter;

    public FilterPropertyKeysExtractor(FilterPredicate filter) {
        this.filter = filter;
    }

    public Set<String> extract() {
        Set<String> ret = new HashSet<>();
        filter.accept(this, ret);
        return ret;
    }

    @Override public Void visit(EqualFilterPredicate filterPredicate, Set<String> result) {
        result.add(filterPredicate.propertyKey);
        return null;
    }

    @Override public Void visit(NotEqualFilterPredicate filterPredicate, Set<String> result) {
        result.add(filterPredicate.propertyKey);
        return null;
    }

    @Override public Void visit(GreaterFilterPredicate filterPredicate, Set<String> result) {
        result.add(filterPredicate.propertyKey);
        return null;
    }

    @Override public Void visit(GreaterOrEqualFilterPredicate filterPredicate, Set<String> result) {
        result.add(filterPredicate.propertyKey);
        return null;
    }

    @Override public Void visit(LessFilterPredicate filterPredicate, Set<String> result) {
        result.add(filterPredicate.propertyKey);
        return null;
    }

    @Override public Void visit(LessOrEqualFilterPredicate filterPredicate, Set<String> result) {
        result.add(filterPredicate.propertyKey);
        return null;
    }

    @Override public Void visit(IsNullFilterPredicate filterPredicate, Set<String> result) {
        result.add(filterPredicate.propertyKey);
        return null;
    }

    @Override public Void visit(IsNotNullFilterPredicate filterPredicate, Set<String> result) {
        result.add(filterPredicate.propertyKey);
        return null;
    }

    @Override public Void visit(RegexFilterPredicate filterPredicate, Set<String> result) {
        result.add(filterPredicate.propertyKey);
        return null;
    }

    @Override public Void visit(CompositeFilterPredicate compositeFilter, Set<String> result) {
        compositeFilter.leftFilter.accept(this, result);
        compositeFilter.rightFilter.accept(this, result);
        return null;
    }
}
