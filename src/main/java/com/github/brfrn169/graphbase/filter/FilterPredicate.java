package com.github.brfrn169.graphbase.filter;


public interface FilterPredicate {
    <T, R> R accept(FilterPredicateVisitor<T, R> visitor, T context);

    final class Builder {
        public static FilterPredicate equal(String propertyKey, Object propertyValue) {
            return new EqualFilterPredicate(propertyKey, propertyValue);
        }

        public static FilterPredicate notEqual(String propertyKey, Object propertyValue) {
            return new NotEqualFilterPredicate(propertyKey, propertyValue);
        }

        public static FilterPredicate greater(String propertyKey, Object propertyValue) {
            return new GreaterFilterPredicate(propertyKey, propertyValue);
        }

        public static FilterPredicate greaterOrEqual(String propertyKey, Object propertyValue) {
            return new GreaterOrEqualFilterPredicate(propertyKey, propertyValue);
        }

        public static FilterPredicate less(String propertyKey, Object propertyValue) {
            return new LessFilterPredicate(propertyKey, propertyValue);
        }

        public static FilterPredicate lessOrEqual(String propertyKey, Object propertyValue) {
            return new LessOrEqualFilterPredicate(propertyKey, propertyValue);
        }

        public static FilterPredicate regex(String propertyKey, String regex) {
            return new RegexFilterPredicate(propertyKey, regex);
        }

        public static FilterPredicate isNull(String propertyKey) {
            return new IsNullFilterPredicate(propertyKey);
        }

        public static FilterPredicate isNotNull(String propertyKey) {
            return new IsNotNullFilterPredicate(propertyKey);
        }

        public static FilterPredicate and(FilterPredicate left, FilterPredicate right) {
            return new CompositeFilterPredicate(left, CompositeFilterPredicate.Operator.AND, right);
        }

        public static FilterPredicate or(FilterPredicate left, FilterPredicate right) {
            return new CompositeFilterPredicate(left, CompositeFilterPredicate.Operator.OR, right);
        }
    }
}
