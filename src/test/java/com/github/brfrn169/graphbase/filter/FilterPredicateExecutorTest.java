package com.github.brfrn169.graphbase.filter;

import com.github.brfrn169.graphbase.Node;
import com.github.brfrn169.graphbase.util.Properties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.github.brfrn169.graphbase.filter.FilterPredicate.Builder.and;
import static com.github.brfrn169.graphbase.filter.FilterPredicate.Builder.equal;
import static com.github.brfrn169.graphbase.filter.FilterPredicate.Builder.greater;
import static com.github.brfrn169.graphbase.filter.FilterPredicate.Builder.greaterOrEqual;
import static com.github.brfrn169.graphbase.filter.FilterPredicate.Builder.isNotNull;
import static com.github.brfrn169.graphbase.filter.FilterPredicate.Builder.isNull;
import static com.github.brfrn169.graphbase.filter.FilterPredicate.Builder.less;
import static com.github.brfrn169.graphbase.filter.FilterPredicate.Builder.lessOrEqual;
import static com.github.brfrn169.graphbase.filter.FilterPredicate.Builder.notEqual;
import static com.github.brfrn169.graphbase.filter.FilterPredicate.Builder.or;
import static com.github.brfrn169.graphbase.filter.FilterPredicate.Builder.regex;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@DisplayName("Tests for FilterPredicateExecutor") public class FilterPredicateExecutorTest {

    @Test @DisplayName("Test") public void test() {
        Node node = new Node("id", "type", Properties.property("prop1", 5, "prop2", "1234567890"));

        assertTrue(new FilterExecutor(equal("prop1", 5)).execute(node));
        assertFalse(new FilterExecutor(equal("prop1", 2)).execute(node));
        assertTrue(new FilterExecutor(greater("prop1", 4)).execute(node));
        assertFalse(new FilterExecutor(greater("prop1", 5)).execute(node));
        assertTrue(new FilterExecutor(greaterOrEqual("prop1", 5)).execute(node));
        assertFalse(new FilterExecutor(greaterOrEqual("prop1", 6)).execute(node));
        assertTrue(new FilterExecutor(less("prop1", 6)).execute(node));
        assertFalse(new FilterExecutor(less("prop1", 5)).execute(node));
        assertTrue(new FilterExecutor(lessOrEqual("prop1", 5)).execute(node));
        assertFalse(new FilterExecutor(lessOrEqual("prop1", 4)).execute(node));
        assertTrue(new FilterExecutor(notEqual("prop1", 4)).execute(node));
        assertFalse(new FilterExecutor(notEqual("prop1", 5)).execute(node));
        assertTrue(new FilterExecutor(regex("prop2", "[0-9]+")).execute(node));
        assertFalse(new FilterExecutor(regex("prop2", "[a-z]+")).execute(node));
        assertTrue(new FilterExecutor(isNull("prop3")).execute(node));
        assertFalse(new FilterExecutor(isNull("prop1")).execute(node));
        assertTrue(new FilterExecutor(isNotNull("prop1")).execute(node));
        assertFalse(new FilterExecutor(isNotNull("prop3")).execute(node));
        assertTrue(
            new FilterExecutor(and(equal("prop1", 5), equal("prop2", "1234567890"))).execute(node));
        assertFalse(
            new FilterExecutor(and(equal("prop1", 3), equal("prop2", "abcdefg"))).execute(node));
        assertTrue(
            new FilterExecutor(or(equal("prop1", 5), equal("prop2", "abcdefg"))).execute(node));
        assertFalse(
            new FilterExecutor(or(equal("prop1", 3), equal("prop2", "abcdefg"))).execute(node));
    }
}
