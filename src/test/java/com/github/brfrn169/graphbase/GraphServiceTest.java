package com.github.brfrn169.graphbase;

import com.github.brfrn169.graphbase.exception.NodeAlreadyExistsException;
import com.github.brfrn169.graphbase.exception.NodeNotFoundException;
import com.github.brfrn169.graphbase.hbase.HBaseGraphStorage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.function.Function;

import static com.github.brfrn169.graphbase.PropertyProjections.Builder.*;
import static org.junit.Assert.*;

@RunWith(Enclosed.class) public abstract class GraphServiceTest {

    private static HBaseTestingUtility testUtil = new HBaseTestingUtility();
    private static GraphService graphService;

    public static void initialize(Function<Configuration, GraphStorage> getGraphStorage)
        throws Exception {
        Configuration conf = testUtil.getConfiguration();
        conf.setBoolean(HBaseGraphStorage.TABLE_COMPRESSION_CONF_KEY, false);
        testUtil.startMiniCluster();

        graphService = new GraphService(conf, getGraphStorage.apply(conf));
    }

    public static void shutdown() throws Exception {
        graphService.close();
        testUtil.shutdownMiniCluster();
    }

    public static class GraphCatalogRelatedTest {
        @Test public void createAndDropGraph() {
            final String graphId = "GraphCatalogRelatedTest-createAndDropGraph";

            graphService.createGraph(new GraphConfiguration(graphId));

            Optional<GraphConfiguration> graphConfiguration =
                graphService.getGraphConfiguration(graphId);
            assertTrue(graphConfiguration.isPresent());
            graphConfiguration
                .ifPresent(graphConf -> assertEquals(graphId, graphConf.getGraphId()));

            graphService.dropGraph(graphId);

            graphConfiguration = graphService.getGraphConfiguration(graphId);
            assertFalse(graphConfiguration.isPresent());
        }
    }


    public static class NodeRelatedTest {
        @Test(expected = NodeAlreadyExistsException.class)
        public void createNodeWhenAlreadyNodeExists() {
            final String graphId = "NodeRelatedTest-createNodeWhenAlreadyNodeExists";
            final String nodeId = "nodeId";
            final String nodeType = "nodeType";

            graphService.createGraph(new GraphConfiguration(graphId));

            graphService.addNode(graphId, nodeId, nodeType, Collections.emptyMap());
            graphService.addNode(graphId, nodeId, nodeType, Collections.emptyMap());
        }

        @Test public void getNode() {
            final String graphId = "NodeRelatedTest-getNode";
            final String nodeId = "nodeId";
            final String nodeType = "nodeType";

            final Map<String, Object> properties = new HashMap<>();
            final String propertyKey = "key";
            properties.put(propertyKey, "value");

            graphService.createGraph(new GraphConfiguration(graphId));
            graphService.addNode(graphId, nodeId, nodeType, properties);

            {
                Optional<Node> node = graphService.getNode(graphId, nodeId, withAllProperties());
                assertTrue(node.isPresent());

                node.ifPresent((n) -> {
                    assertEquals(nodeId, n.getId());
                    assertEquals(nodeType, n.getType());

                    assertEquals(properties.size() + 1, n.getProperties().size());
                    assertEquals(properties.get(propertyKey), n.getProperties().get(propertyKey));
                    assertTrue(n.getProperties().containsKey(GraphbaseConstants.PROPERTY_ADD_AT));
                });
            }

            {
                Optional<Node> node = graphService.getNode(graphId, nodeId, withoutProperties());
                assertTrue(node.isPresent());

                node.ifPresent((n) -> {
                    assertEquals(nodeId, n.getId());
                    assertEquals(nodeType, n.getType());
                    assertEquals(0, n.getProperties().size());
                });
            }

            {
                Optional<Node> node =
                    graphService.getNode(graphId, nodeId, withProperties(propertyKey));
                assertTrue(node.isPresent());

                node.ifPresent((n) -> {
                    assertEquals(nodeId, n.getId());
                    assertEquals(nodeType, n.getType());

                    assertEquals(properties.size(), n.getProperties().size());
                    assertEquals(properties.get(propertyKey), n.getProperties().get(propertyKey));
                });
            }
        }

        @Test public void getNodeWhenNodeNotExists() {
            final String graphId = "NodeRelatedTest-getNodeWhenNodeNotExists";
            final String nodeId = "nodeId";

            graphService.createGraph(new GraphConfiguration(graphId));

            Optional<Node> node = graphService.getNode(graphId, nodeId, withoutProperties());
            assertFalse(node.isPresent());
        }

        @Test public void updateNode() {
            final String graphId = "NodeRelatedTest-updateNode";
            final String nodeId = "nodeId";
            final String nodeType = "nodeType";

            final String propertyKey1 = "key1";
            final String propertyKey2 = "key2";

            final Map<String, Object> properties = new HashMap<>();
            properties.put(propertyKey1, "value1");
            properties.put(propertyKey2, "value2");

            graphService.createGraph(new GraphConfiguration(graphId));

            graphService.addNode(graphId, nodeId, nodeType, properties);

            final Map<String, Object> updateProperties = new HashMap<>();
            updateProperties.put(propertyKey1, "value3");

            final Set<String> deleteKeys = new HashSet<>();
            deleteKeys.add(propertyKey2);

            graphService.updateNode(graphId, nodeId, updateProperties, deleteKeys);

            Optional<Node> node = graphService.getNode(graphId, nodeId, withAllProperties());
            assertTrue(node.isPresent());

            node.ifPresent((n) -> {
                assertEquals(nodeId, n.getId());
                assertEquals(nodeType, n.getType());

                assertEquals(2, n.getProperties().size());
                assertEquals(updateProperties.get(propertyKey1),
                    n.getProperties().get(propertyKey1));
                assertFalse(n.getProperties().containsKey(propertyKey2));
                assertTrue(n.getProperties().containsKey(GraphbaseConstants.PROPERTY_ADD_AT));
            });
        }

        @Test(expected = NodeNotFoundException.class) public void updateNodeWhenNodeNotExists() {
            final String graphId = "NodeRelatedTest-updateNodeWhenNodeNotExists";
            final String nodeId = "nodeId";

            graphService.createGraph(new GraphConfiguration(graphId));

            final Map<String, Object> updatedProperties = new HashMap<>();
            updatedProperties.put("key1", "value");

            final Set<String> deletedKeys = new HashSet<>();
            deletedKeys.add("key2");

            graphService.updateNode(graphId, nodeId, updatedProperties, deletedKeys);
        }

        @Test public void deleteNode() {
            final String graphId = "NodeRelatedTest-deleteNode";
            final String nodeId = "nodeId";
            final String nodeType = "nodeType";

            graphService.createGraph(new GraphConfiguration(graphId));

            graphService.addNode(graphId, nodeId, nodeType, Collections.emptyMap());

            graphService.deleteNode(graphId, nodeId);

            Optional<Node> node = graphService.getNode(graphId, nodeId, withoutProperties());
            assertFalse(node.isPresent());
        }

        @Test(expected = NodeNotFoundException.class) public void deleteNodeWhenNodeNotExists() {
            final String graphId = "NodeRelatedTest-deleteNodeWhenNodeNotExists";
            final String nodeId = "nodeId";

            graphService.createGraph(new GraphConfiguration(graphId));

            graphService.deleteNode(graphId, nodeId);
        }
    }
}
