package com.github.brfrn169.graphbase.sort;


import com.github.brfrn169.graphbase.Entity;
import com.github.brfrn169.graphbase.util.Properties;

import java.util.Comparator;
import java.util.List;

public class SortComparator implements Comparator<Entity> {

    private final List<SortPredicate> sorts;

    public SortComparator(List<SortPredicate> sorts) {
        this.sorts = sorts;
    }

    @Override public int compare(Entity left, Entity right) {
        for (SortPredicate sort : sorts) {
            String propertyKey = sort.getPropertyKey();

            Object leftPropertyValue = left.propertyValue(propertyKey);
            Object rightPropertyValue = right.propertyValue(propertyKey);

            if (leftPropertyValue != null && rightPropertyValue == null) {
                return 1;
            } else if (leftPropertyValue == null && rightPropertyValue != null) {
                return -1;
            } else if (leftPropertyValue == null) {
                return 0;
            }

            if (leftPropertyValue instanceof Comparable
                && !(rightPropertyValue instanceof Comparable)) {
                return 1;
            } else if (!(leftPropertyValue instanceof Comparable)
                && rightPropertyValue instanceof Comparable) {
                return -1;
            } else if (!(leftPropertyValue instanceof Comparable)) {
                return 0;
            }

            int compare = Properties.comparePropertyValue(leftPropertyValue, rightPropertyValue);
            if (compare == 0) {
                continue;
            }

            switch (sort.getOperator()) {
                case ASC:
                    return compare;
                case DESC:
                    return -compare;
                default:
                    throw new AssertionError();
            }
        }
        return 0;
    }
}
