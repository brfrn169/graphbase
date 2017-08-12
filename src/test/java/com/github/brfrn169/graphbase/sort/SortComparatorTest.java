package com.github.brfrn169.graphbase.sort;

import com.github.brfrn169.graphbase.Node;
import com.github.brfrn169.graphbase.util.Properties;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.brfrn169.graphbase.sort.SortPredicate.Builder.asc;
import static com.github.brfrn169.graphbase.sort.SortPredicate.Builder.desc;
import static org.junit.Assert.assertEquals;

public class SortComparatorTest {

    @Test public void test() {
        Node node1 = new Node("id1", "type", Properties.property("prop1", 3, "prop2", 1));
        Node node2 = new Node("id2", "type", Properties.property("prop1", 5, "prop2", 5));
        Node node3 = new Node("id3", "type", Properties.property("prop1", 1, "prop2", 3));
        Node node4 = new Node("id4", "type", Properties.property("prop1", 3, "prop2", 3));

        List<Node> nodes = Arrays.asList(node1, node2, node3, node4);

        {
            SortComparator sortComparator =
                new SortComparator(Collections.singletonList(asc("prop1")));
            List<Node> result = nodes.stream().sorted(sortComparator).collect(Collectors.toList());

            assertEquals(node3, result.get(0));
            assertEquals(node1, result.get(1));
            assertEquals(node4, result.get(2));
            assertEquals(node2, result.get(3));
        }
        {
            SortComparator sortComparator =
                new SortComparator(Collections.singletonList(desc("prop1")));
            List<Node> result = nodes.stream().sorted(sortComparator).collect(Collectors.toList());

            assertEquals(node2, result.get(0));
            assertEquals(node1, result.get(1));
            assertEquals(node4, result.get(2));
            assertEquals(node3, result.get(3));
        }
        {
            SortComparator sortComparator =
                new SortComparator(Arrays.asList(asc("prop1"), desc("prop2")));
            List<Node> result = nodes.stream().sorted(sortComparator).collect(Collectors.toList());

            assertEquals(node3, result.get(0));
            assertEquals(node4, result.get(1));
            assertEquals(node1, result.get(2));
            assertEquals(node2, result.get(3));
        }
    }
}
