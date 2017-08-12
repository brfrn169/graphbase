package com.github.brfrn169.graphbase.filter;

import com.github.brfrn169.graphbase.Entity;
import com.github.brfrn169.graphbase.util.Properties;

import java.util.regex.Pattern;

public class FilterExecutor implements FilterPredicateVisitor<Entity, Boolean> {

    private final FilterPredicate filter;

    public FilterExecutor(FilterPredicate filter) {
        this.filter = filter;
    }

    public boolean execute(Entity target) {
        return filter.accept(this, target);
    }

    @Override public Boolean visit(EqualFilterPredicate filterPredicate, Entity target) {
        Object value = target.getPropertyValue(filterPredicate.propertyKey);
        return value != null
            && Properties.comparePropertyValue(value, filterPredicate.propertyValue) == 0;
    }

    @Override public Boolean visit(NotEqualFilterPredicate filterPredicate, Entity target) {
        Object value = target.getPropertyValue(filterPredicate.propertyKey);
        return value != null
            && Properties.comparePropertyValue(value, filterPredicate.propertyValue) != 0;
    }

    @Override public Boolean visit(GreaterFilterPredicate filterPredicate, Entity target) {
        Object value = target.getPropertyValue(filterPredicate.propertyKey);
        return value != null
            && Properties.comparePropertyValue(value, filterPredicate.propertyValue) > 0;
    }

    @Override public Boolean visit(GreaterOrEqualFilterPredicate filterPredicate, Entity target) {
        Object value = target.getPropertyValue(filterPredicate.propertyKey);
        return value != null
            && Properties.comparePropertyValue(value, filterPredicate.propertyValue) >= 0;
    }

    @Override public Boolean visit(LessFilterPredicate filterPredicate, Entity target) {
        Object value = target.getPropertyValue(filterPredicate.propertyKey);
        return value != null
            && Properties.comparePropertyValue(value, filterPredicate.propertyValue) < 0;
    }

    @Override public Boolean visit(LessOrEqualFilterPredicate filterPredicate, Entity target) {
        Object value = target.getPropertyValue(filterPredicate.propertyKey);
        return value != null
            && Properties.comparePropertyValue(value, filterPredicate.propertyValue) <= 0;
    }

    @Override public Boolean visit(IsNullFilterPredicate filterPredicate, Entity target) {
        return target.getPropertyValue(filterPredicate.propertyKey) == null;
    }

    @Override public Boolean visit(IsNotNullFilterPredicate filterPredicate, Entity target) {
        return target.getPropertyValue(filterPredicate.propertyKey) != null;
    }

    @Override public Boolean visit(RegexFilterPredicate filterPredicate, Entity target) {
        Object value = target.getPropertyValue(filterPredicate.propertyKey);
        return value != null && Pattern.compile(filterPredicate.regex).matcher((String) value)
            .matches();
    }

    @Override public Boolean visit(CompositeFilterPredicate compositeFilter, Entity target) {
        boolean leftRet = compositeFilter.leftFilter.accept(this, target);
        boolean rightRet = compositeFilter.rightFilter.accept(this, target);

        switch (compositeFilter.operator) {
            case OR:
                return leftRet || rightRet;
            case AND:
                return leftRet && rightRet;
            default:
                throw new AssertionError();
        }
    }
}
