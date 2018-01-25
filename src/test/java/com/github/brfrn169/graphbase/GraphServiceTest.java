package com.github.brfrn169.graphbase;

import com.github.brfrn169.graphbase.exception.NodeAlreadyExistsException;
import com.github.brfrn169.graphbase.exception.NodeNotFoundException;
import com.github.brfrn169.graphbase.exception.RelationshipAlreadyExistsException;
import com.github.brfrn169.graphbase.exception.RelationshipNotFoundException;
import com.github.brfrn169.graphbase.hbase.HBaseGraphStorage;
import com.github.brfrn169.graphbase.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.brfrn169.graphbase.PropertyProjections.Builder.withAllProperties;
import static com.github.brfrn169.graphbase.PropertyProjections.Builder.withProperties;
import static com.github.brfrn169.graphbase.PropertyProjections.Builder.withoutProperties;
import static com.github.brfrn169.graphbase.filter.FilterPredicate.Builder.greater;
import static com.github.brfrn169.graphbase.filter.FilterPredicate.Builder.greaterOrEqual;
import static com.github.brfrn169.graphbase.sort.SortPredicate.Builder.asc;
import static com.github.brfrn169.graphbase.sort.SortPredicate.Builder.desc;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.junit.Assert.assertThat;

@RunWith(Enclosed.class) public abstract class GraphServiceTest {

    private static HBaseTestingUtility testUtil = new HBaseTestingUtility();
    private static GraphService graphService;

    protected static void initialize(Function<Configuration, GraphStorage> getGraphStorage)
        throws Exception {
        Configuration conf = testUtil.getConfiguration();
        conf.setBoolean(HBaseGraphStorage.TABLE_COMPRESSION_CONF_KEY, false);
        testUtil.startMiniCluster();

        graphService = new GraphService(conf, getGraphStorage.apply(conf));
    }

    protected static void shutdown() throws Exception {
        graphService.close();
        testUtil.shutdownMiniCluster();
    }

    private static void createGraph(String graphId) {
        graphService.createGraph(new GraphConfiguration(graphId));

        // waiting for synchronizing with zookeeper
        while (true) {
            if (graphService.getGraphConfiguration(graphId).isPresent()) {
                return;
            }

            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class GraphCatalogRelatedTest {
        @Test public void createAndDropGraph() {
            final String graphId = "GraphCatalogRelatedTest-createAndDropGraph";

            createGraph(graphId);

            Optional<GraphConfiguration> graphConfiguration =
                graphService.getGraphConfiguration(graphId);
            assertThat(graphConfiguration.isPresent(), is(true));
            graphConfiguration.ifPresent(graphConf -> assertThat(graphConf.graphId(), is(graphId)));

            graphService.dropGraph(graphId);

            graphConfiguration = graphService.getGraphConfiguration(graphId);
            assertThat(graphConfiguration.isPresent(), is(false));
        }
    }


    public static class NodeRelatedTest {
        @Test(expected = NodeAlreadyExistsException.class)
        public void createNodeWhenAlreadyNodeExists() {
            final String graphId = "NodeRelatedTest-createNodeWhenAlreadyNodeExists";
            final String nodeId = "nodeId";
            final String nodeType = "nodeType";

            createGraph(graphId);

            graphService.addNode(graphId, nodeId, nodeType, Collections.emptyMap());
            graphService.addNode(graphId, nodeId, nodeType, Collections.emptyMap());
        }

        @Test public void updateNode() {
            final String graphId = "NodeRelatedTest-updateNode";
            final String nodeId = "nodeId";
            final String nodeType = "nodeType";

            final String propertyKey1 = "key1";
            final String propertyKey2 = "key2";
            final Map<String, Object> properties =
                Properties.property(propertyKey1, "value1", propertyKey2, "value2");

            createGraph(graphId);

            graphService.addNode(graphId, nodeId, nodeType, properties);

            final Map<String, Object> updateProperties = new HashMap<>();
            updateProperties.put(propertyKey1, "value3");

            final Set<String> deleteKeys = new HashSet<>();
            deleteKeys.add(propertyKey2);

            graphService.updateNode(graphId, nodeId, updateProperties, deleteKeys);

            Optional<Node> node = graphService.getNode(graphId, nodeId, withAllProperties());
            assertThat(node.isPresent(), is(true));

            node.ifPresent((n) -> {
                assertThat(n.id(), is(nodeId));
                assertThat(n.type(), is(nodeType));

                assertThat(n.properties().entrySet(), hasSize(2));
                assertThat(n.properties(),
                    hasEntry(propertyKey1, updateProperties.get(propertyKey1)));
                assertThat(n.properties(), not(hasKey(propertyKey2)));
                assertThat(n.properties(), hasKey(GraphbaseConstants.PROPERTY_ADD_AT));
            });
        }

        @Test(expected = NodeNotFoundException.class) public void updateNodeWhenNodeNotExists() {
            final String graphId = "NodeRelatedTest-updateNodeWhenNodeNotExists";
            final String nodeId = "nodeId";

            createGraph(graphId);

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

            createGraph(graphId);

            graphService.addNode(graphId, nodeId, nodeType, Collections.emptyMap());

            graphService.deleteNode(graphId, nodeId);

            Optional<Node> node = graphService.getNode(graphId, nodeId, withoutProperties());
            assertThat(node.isPresent(), is(false));
        }

        @Test(expected = NodeNotFoundException.class) public void deleteNodeWhenNodeNotExists() {
            final String graphId = "NodeRelatedTest-deleteNodeWhenNodeNotExists";
            final String nodeId = "nodeId";

            createGraph(graphId);

            graphService.deleteNode(graphId, nodeId);
        }

        @Test public void getNode() {
            final String graphId = "NodeRelatedTest-getNode";
            final String nodeId = "nodeId";
            final String nodeType = "nodeType";

            final String propertyKey = "key";
            final Map<String, Object> properties = Properties.property(propertyKey, "value");

            createGraph(graphId);

            graphService.addNode(graphId, nodeId, nodeType, properties);

            {
                Optional<Node> node = graphService.getNode(graphId, nodeId, withAllProperties());
                assertThat(node.isPresent(), is(true));

                node.ifPresent((n) -> {
                    assertThat(n.id(), is(nodeId));
                    assertThat(n.type(), is(nodeType));

                    assertThat(n.properties().entrySet(), hasSize(properties.size() + 1));
                    assertThat(n.properties(), hasEntry(propertyKey, properties.get(propertyKey)));
                    assertThat(n.properties(), hasKey(GraphbaseConstants.PROPERTY_ADD_AT));
                });
            }

            {
                Optional<Node> node = graphService.getNode(graphId, nodeId, withoutProperties());
                assertThat(node.isPresent(), is(true));

                node.ifPresent((n) -> {
                    assertThat(n.id(), is(nodeId));
                    assertThat(n.type(), is(nodeType));
                    assertThat(n.properties().entrySet(), is(empty()));
                });
            }

            {
                Optional<Node> node =
                    graphService.getNode(graphId, nodeId, withProperties(propertyKey));
                assertThat(node.isPresent(), is(true));

                node.ifPresent((n) -> {
                    assertThat(n.id(), is(nodeId));
                    assertThat(n.type(), is(nodeType));

                    assertThat(n.properties().entrySet(), hasSize(properties.size()));
                    assertThat(n.properties(), hasEntry(propertyKey, properties.get(propertyKey)));
                });
            }
        }

        @Test public void getNodeWhenNodeNotExists() {
            final String graphId = "NodeRelatedTest-getNodeWhenNodeNotExists";
            final String nodeId = "nodeId";

            createGraph(graphId);

            Optional<Node> node = graphService.getNode(graphId, nodeId, withoutProperties());
            assertThat(node.isPresent(), is(false));
        }

        @Test public void nodeExists() {
            final String graphId = "NodeRelatedTest-nodeExists";

            createGraph(graphId);

            graphService.addNode(graphId, "nodeId", "nodeType", Collections.emptyMap());
            assertThat(graphService.nodeExists(graphId, "nodeId"), is(true));
            assertThat(graphService.nodeExists(graphId, "nodeId2"), is(false));
        }

        @Test public void getNodesWithSpecifyingType() {
            final String graphId = "NodeRelatedTest-getNodesWithSpecifyingType";
            final String nodeIdPrefix = "nodeId";
            final String nodeType1 = "nodeType1";
            final String nodeType2 = "nodeType2";

            createGraph(graphId);

            IntStream.range(0, 3).forEach(i -> graphService
                .addNode(graphId, nodeIdPrefix + i, nodeType1, Collections.emptyMap()));

            IntStream.range(3, 5).forEach(i -> graphService
                .addNode(graphId, nodeIdPrefix + i, nodeType2, Collections.emptyMap()));

            {
                List<String> nodeIds =
                    graphService.getNodes(graphId, null, null, null, withoutProperties()).stream()
                        .map(Node::id).collect(Collectors.toList());

                assertThat(nodeIds, hasSize(5));
                assertThat(nodeIds, hasItems(
                    IntStream.range(0, 5).mapToObj(i -> nodeIdPrefix + i).toArray(String[]::new)));
            }

            {
                Set<String> nodeIds = graphService
                    .getNodes(graphId, Arrays.asList(nodeType1, nodeType2), null, null,
                        withoutProperties()).stream().map(Node::id).collect(Collectors.toSet());

                assertThat(nodeIds, hasSize(5));
                assertThat(nodeIds, hasItems(
                    IntStream.range(0, 5).mapToObj(i -> nodeIdPrefix + i).toArray(String[]::new)));
            }

            {
                Set<String> nodeIds = graphService
                    .getNodes(graphId, Collections.singletonList(nodeType1), null, null,
                        withoutProperties()).stream().map(Node::id).collect(Collectors.toSet());

                assertThat(nodeIds, hasSize(3));
                assertThat(nodeIds, hasItems(
                    IntStream.range(0, 3).mapToObj(i -> nodeIdPrefix + i).toArray(String[]::new)));
            }

            {
                Set<String> nodeIds = new HashSet<>();
                graphService.getNodes(graphId, Collections.singletonList(nodeType2), null, null,
                    withoutProperties()).forEach(node -> nodeIds.add(node.id()));

                assertThat(nodeIds, hasSize(2));
                assertThat(nodeIds, hasItems(
                    IntStream.range(3, 5).mapToObj(i -> nodeIdPrefix + i).toArray(String[]::new)));
            }
        }

        @Test public void getNodesWithSpecifyingFilter() {
            final String graphId = "NodeRelatedTest-getNodesWithSpecifyingFilter";
            final String nodeType = "nodeType";
            final String propertyKey = "prop";

            createGraph(graphId);

            graphService.addNode(graphId, "nodeId1", nodeType, Properties.property(propertyKey, 3));
            graphService.addNode(graphId, "nodeId2", nodeType, Properties.property(propertyKey, 5));
            graphService.addNode(graphId, "nodeId3", nodeType, Properties.property(propertyKey, 1));

            {
                List<Node> result = graphService
                    .getNodes(graphId, null, greater(propertyKey, 3), null, withoutProperties());

                assertThat(result, hasSize(1));
                assertThat(result.get(0).id(), is("nodeId2"));
                assertThat(result.get(0).properties().entrySet(), is(empty()));
            }

            {
                List<Node> result = graphService
                    .getNodes(graphId, null, greaterOrEqual(propertyKey, 3), null,
                        withoutProperties());

                assertThat(result, hasSize(2));
                assertThat(result.get(0).id(), is("nodeId1"));
                assertThat(result.get(0).properties().entrySet(), is(empty()));
                assertThat(result.get(1).id(), is("nodeId2"));
                assertThat(result.get(1).properties().entrySet(), is(empty()));
            }
        }

        @Test public void getNodesWithSpecifyingSort() {
            final String graphId = "NodeRelatedTest-getNodesWithSpecifyingSort";
            final String nodeType = "nodeType";
            final String propertyKey = "prop";

            createGraph(graphId);

            graphService.addNode(graphId, "nodeId1", nodeType, Properties.property(propertyKey, 3));
            graphService.addNode(graphId, "nodeId2", nodeType, Properties.property(propertyKey, 5));
            graphService.addNode(graphId, "nodeId3", nodeType, Properties.property(propertyKey, 1));

            {
                List<Node> result = graphService
                    .getNodes(graphId, null, null, Collections.singletonList(asc(propertyKey)),
                        withoutProperties());

                assertThat(result, hasSize(3));
                assertThat(result.get(0).id(), is("nodeId3"));
                assertThat(result.get(0).properties().entrySet(), is(empty()));
                assertThat(result.get(1).id(), is("nodeId1"));
                assertThat(result.get(1).properties().entrySet(), is(empty()));
                assertThat(result.get(2).id(), is("nodeId2"));
                assertThat(result.get(2).properties().entrySet(), is(empty()));
            }

            {
                List<Node> result = graphService
                    .getNodes(graphId, null, null, Collections.singletonList(desc(propertyKey)),
                        withoutProperties());

                assertThat(result, hasSize(3));
                assertThat(result.get(0).id(), is("nodeId2"));
                assertThat(result.get(0).properties().entrySet(), is(empty()));
                assertThat(result.get(1).id(), is("nodeId1"));
                assertThat(result.get(1).properties().entrySet(), is(empty()));
                assertThat(result.get(2).id(), is("nodeId3"));
                assertThat(result.get(2).properties().entrySet(), is(empty()));
            }
        }
    }


    public static class RelationshipRelatedTest {
        @Test(expected = RelationshipAlreadyExistsException.class)
        public void createRelationshipWhenAlreadyRelationshipExists() {
            final String graphId =
                "RelationshipRelatedTest-createRelationshipWhenAlreadyRelationshipExists";
            final String outNodeId = "outNodeId";
            final String relType = "relType";
            final String inNodeId = "inNodeId";

            createGraph(graphId);

            graphService
                .addRelationship(graphId, outNodeId, relType, inNodeId, Collections.emptyMap());
            graphService
                .addRelationship(graphId, outNodeId, relType, inNodeId, Collections.emptyMap());
        }

        @Test public void updateRelationship() {
            final String graphId = "RelationshipRelatedTest-updateRelationship";
            final String outNodeId = "outNodeId";
            final String relType = "relType";
            final String inNodeId = "inNodeId";

            final String propertyKey1 = "key1";
            final String propertyKey2 = "key2";
            final Map<String, Object> properties =
                Properties.property(propertyKey1, "value1", propertyKey2, "value2");

            createGraph(graphId);

            graphService.addRelationship(graphId, outNodeId, relType, inNodeId, properties);

            final Map<String, Object> updateProperties = new HashMap<>();
            updateProperties.put(propertyKey1, "value3");

            final Set<String> deleteKeys = new HashSet<>();
            deleteKeys.add(propertyKey2);

            graphService.updateRelationship(graphId, outNodeId, relType, inNodeId, updateProperties,
                deleteKeys);

            Optional<Relationship> rel = graphService
                .getRelationship(graphId, outNodeId, relType, inNodeId, withAllProperties());
            assertThat(rel.isPresent(), is(true));

            rel.ifPresent((r) -> {
                assertThat(r.outNodeId(), is(outNodeId));
                assertThat(r.type(), is(relType));
                assertThat(r.inNodeId(), is(inNodeId));

                assertThat(r.properties().entrySet(), hasSize(2));
                assertThat(r.properties(),
                    hasEntry(propertyKey1, updateProperties.get(propertyKey1)));
                assertThat(r.properties(), not(hasKey(propertyKey2)));
                assertThat(r.properties(), hasKey(GraphbaseConstants.PROPERTY_ADD_AT));
            });
        }

        @Test(expected = RelationshipNotFoundException.class)
        public void updateRelationshipWhenRelationshipNotExists() {
            final String graphId =
                "RelationshipRelatedTest-updateRelationshipWhenRelationshipNotExists";
            final String outNodeId = "outNodeId";
            final String relType = "relType";
            final String inNodeId = "inNodeId";

            createGraph(graphId);

            final Map<String, Object> updatedProperties = new HashMap<>();
            updatedProperties.put("key1", "value");

            final Set<String> deletedKeys = new HashSet<>();
            deletedKeys.add("key2");

            graphService
                .updateRelationship(graphId, outNodeId, relType, inNodeId, updatedProperties,
                    deletedKeys);
        }

        @Test public void deleteRelationship() {
            final String graphId = "RelationshipRelatedTest-deleteRelationship";
            final String outNodeId = "outNodeId";
            final String relType = "relType";
            final String inNodeId = "inNodeId";

            createGraph(graphId);

            graphService
                .addRelationship(graphId, outNodeId, relType, inNodeId, Collections.emptyMap());

            graphService.deleteRelationship(graphId, outNodeId, relType, inNodeId);

            Optional<Relationship> rel = graphService
                .getRelationship(graphId, outNodeId, relType, inNodeId, withoutProperties());
            assertThat(rel.isPresent(), is(false));
        }

        @Test(expected = RelationshipNotFoundException.class)
        public void deleteRelationshipWhenRelationshipNotExists() {
            final String graphId =
                "RelationshipRelatedTest-deleteRelationshipWhenRelationshipNotExists";
            final String outNodeId = "outNodeId";
            final String relType = "relType";
            final String inNodeId = "inNodeId";

            createGraph(graphId);

            graphService.deleteRelationship(graphId, outNodeId, relType, inNodeId);
        }

        @Test public void getRelationship() {
            final String graphId = "RelationshipRelatedTest-getRelationship";
            final String outNodeId = "outNodeId";
            final String relType = "relType";
            final String inNodeId = "inNodeId";

            final String propertyKey = "key";
            final Map<String, Object> properties = Properties.property(propertyKey, "value");

            createGraph(graphId);
            graphService.addRelationship(graphId, outNodeId, relType, inNodeId, properties);

            {
                Optional<Relationship> rel = graphService
                    .getRelationship(graphId, outNodeId, relType, inNodeId, withAllProperties());
                assertThat(rel.isPresent(), is(true));

                rel.ifPresent((r) -> {
                    assertThat(r.outNodeId(), is(outNodeId));
                    assertThat(r.type(), is(relType));
                    assertThat(r.inNodeId(), is(inNodeId));

                    assertThat(r.properties().entrySet(), hasSize(properties.size() + 1));
                    assertThat(r.properties(), hasEntry(propertyKey, properties.get(propertyKey)));
                    assertThat(r.properties(), hasKey(GraphbaseConstants.PROPERTY_ADD_AT));
                });
            }

            {
                Optional<Relationship> rel = graphService
                    .getRelationship(graphId, outNodeId, relType, inNodeId, withoutProperties());
                assertThat(rel.isPresent(), is(true));

                rel.ifPresent((r) -> {
                    assertThat(r.outNodeId(), is(outNodeId));
                    assertThat(r.type(), is(relType));
                    assertThat(r.inNodeId(), is(inNodeId));
                    assertThat(r.properties().entrySet(), is(empty()));
                });
            }

            {
                Optional<Relationship> rel = graphService
                    .getRelationship(graphId, outNodeId, relType, inNodeId,
                        withProperties(propertyKey));
                assertThat(rel.isPresent(), is(true));

                rel.ifPresent((r) -> {
                    assertThat(r.outNodeId(), is(outNodeId));
                    assertThat(r.type(), is(relType));
                    assertThat(r.inNodeId(), is(inNodeId));

                    assertThat(r.properties().entrySet(), hasSize(properties.size()));
                    assertThat(r.properties(), hasEntry(propertyKey, properties.get(propertyKey)));
                });
            }
        }

        @Test public void getRelationshipWhenRelationshipNotExists() {
            final String graphId =
                "RelationshipRelatedTest-getRelationshipWhenRelationshipNotExists";
            final String outNodeId = "outNodeId";
            final String relType = "relType";
            final String inNodeId = "inNodeId";

            createGraph(graphId);

            Optional<Relationship> rel = graphService
                .getRelationship(graphId, outNodeId, relType, inNodeId, withoutProperties());
            assertThat(rel.isPresent(), is(false));
        }

        @Test public void relationshipExists() {
            final String graphId = "RelationshipRelatedTest-relationshipExists";

            createGraph(graphId);

            graphService.addRelationship(graphId, "outNodeId", "relType", "inNodeId",
                Collections.emptyMap());

            assertThat(graphService.relationshipExists(graphId, "outNodeId", "relType", "inNodeId"),
                is(true));
            assertThat(
                graphService.relationshipExists(graphId, "outNodeId2", "relType", "inNodeId2"),
                is(false));
        }

        @Test public void getRelationshipsWithSpecifyingType() {
            final String graphId = "RelationshipRelatedTest-getRelationshipsWithSpecifyingType";
            final String outNodeIdPrefix = "outNodeId";
            final String inNodeIdPrefix = "inNodeId";
            final String relType1 = "relType1";
            final String relType2 = "relType2";

            createGraph(graphId);

            IntStream.range(0, 3).forEach(i -> graphService
                .addRelationship(graphId, outNodeIdPrefix + i, relType1, inNodeIdPrefix + i,
                    Collections.emptyMap()));

            IntStream.range(3, 5).forEach(i -> graphService
                .addRelationship(graphId, outNodeIdPrefix + i, relType2, inNodeIdPrefix + i,
                    Collections.emptyMap()));

            {
                List<Relationship> relationships =
                    graphService.getRelationships(graphId, null, null, null, withoutProperties());

                assertThat(relationships, hasSize(5));

                Relationship[] expected = Stream.concat(IntStream.range(0, 3).mapToObj(
                    i -> new Relationship(outNodeIdPrefix + i, relType1, inNodeIdPrefix + i,
                        Collections.emptyMap())), IntStream.range(3, 5).mapToObj(
                    i -> new Relationship(outNodeIdPrefix + i, relType2, inNodeIdPrefix + i,
                        Collections.emptyMap()))).toArray(Relationship[]::new);
                assertThat(relationships, hasItems(expected));
            }

            {
                List<Relationship> relationships = graphService
                    .getRelationships(graphId, Arrays.asList(relType1, relType2), null, null,
                        withoutProperties());

                assertThat(relationships, hasSize(5));

                Relationship[] expected = Stream.concat(IntStream.range(0, 3).mapToObj(
                    i -> new Relationship(outNodeIdPrefix + i, relType1, inNodeIdPrefix + i,
                        Collections.emptyMap())), IntStream.range(3, 5).mapToObj(
                    i -> new Relationship(outNodeIdPrefix + i, relType2, inNodeIdPrefix + i,
                        Collections.emptyMap()))).toArray(Relationship[]::new);
                assertThat(relationships, hasItems(expected));
            }

            {
                List<Relationship> relationships = graphService
                    .getRelationships(graphId, Collections.singletonList(relType1), null, null,
                        withoutProperties());

                assertThat(relationships, hasSize(3));

                Relationship[] expected = IntStream.range(0, 3).mapToObj(
                    i -> new Relationship(outNodeIdPrefix + i, relType1, inNodeIdPrefix + i,
                        Collections.emptyMap())).toArray(Relationship[]::new);
                assertThat(relationships, hasItems(expected));
            }

            {
                List<Relationship> relationships = graphService
                    .getRelationships(graphId, Collections.singletonList(relType2), null, null,
                        withoutProperties());

                assertThat(relationships, hasSize(2));

                Relationship[] expected = IntStream.range(3, 5).mapToObj(
                    i -> new Relationship(outNodeIdPrefix + i, relType2, inNodeIdPrefix + i,
                        Collections.emptyMap())).toArray(Relationship[]::new);
                assertThat(relationships, hasItems(expected));
            }
        }

        @Test public void getRelationshipsWithSpecifyingFilter() {
            final String graphId = "RelationshipRelatedTest-getRelationshipsWithSpecifyingFilter";
            final String relType = "relType";
            final String propertyKey = "prop";

            createGraph(graphId);

            graphService.addRelationship(graphId, "outNodeId1", relType, "inNodeId1",
                Properties.property(propertyKey, 3));
            graphService.addRelationship(graphId, "outNodeId2", relType, "inNodeId2",
                Properties.property(propertyKey, 5));
            graphService.addRelationship(graphId, "outNodeId3", relType, "inNodeId3",
                Properties.property(propertyKey, 1));


            {
                List<Relationship> result = graphService
                    .getRelationships(graphId, null, greater(propertyKey, 3), null,
                        withoutProperties());

                assertThat(result, hasSize(1));

                assertThat(result.get(0).outNodeId(), is("outNodeId2"));
                assertThat(result.get(0).type(), is(relType));
                assertThat(result.get(0).inNodeId(), is("inNodeId2"));
                assertThat(result.get(0).properties().entrySet(), is(empty()));
            }

            {
                List<Relationship> result = graphService
                    .getRelationships(graphId, null, greaterOrEqual(propertyKey, 3), null,
                        withoutProperties());

                assertThat(result, hasSize(2));

                assertThat(result.get(0).outNodeId(), is("outNodeId2"));
                assertThat(result.get(0).type(), is(relType));
                assertThat(result.get(0).inNodeId(), is("inNodeId2"));
                assertThat(result.get(0).properties().entrySet(), is(empty()));

                assertThat(result.get(1).outNodeId(), is("outNodeId1"));
                assertThat(result.get(1).type(), is(relType));
                assertThat(result.get(1).inNodeId(), is("inNodeId1"));
                assertThat(result.get(1).properties().entrySet(), is(empty()));
            }
        }

        @Test public void getRelationshipsWithSpecifyingSort() {
            final String graphId = "RelationshipRelatedTest-getRelationshipsWithSpecifyingSort";
            final String relType = "relType";
            final String propertyKey = "prop";

            createGraph(graphId);

            graphService.addRelationship(graphId, "outNodeId1", relType, "inNodeId1",
                Properties.property(propertyKey, 3));
            graphService.addRelationship(graphId, "outNodeId2", relType, "inNodeId2",
                Properties.property(propertyKey, 5));
            graphService.addRelationship(graphId, "outNodeId3", relType, "inNodeId3",
                Properties.property(propertyKey, 1));

            {
                List<Relationship> result = graphService.getRelationships(graphId, null, null,
                    Collections.singletonList(asc(propertyKey)), withoutProperties());

                assertThat(result, hasSize(3));

                assertThat(result.get(0).outNodeId(), is("outNodeId3"));
                assertThat(result.get(0).type(), is(relType));
                assertThat(result.get(0).inNodeId(), is("inNodeId3"));
                assertThat(result.get(0).properties().entrySet(), is(empty()));

                assertThat(result.get(1).outNodeId(), is("outNodeId1"));
                assertThat(result.get(1).type(), is(relType));
                assertThat(result.get(1).inNodeId(), is("inNodeId1"));
                assertThat(result.get(1).properties().entrySet(), is(empty()));

                assertThat(result.get(2).outNodeId(), is("outNodeId2"));
                assertThat(result.get(2).type(), is(relType));
                assertThat(result.get(2).inNodeId(), is("inNodeId2"));
                assertThat(result.get(2).properties().entrySet(), is(empty()));
            }

            {
                List<Relationship> result = graphService.getRelationships(graphId, null, null,
                    Collections.singletonList(desc(propertyKey)), withoutProperties());

                assertThat(result, hasSize(3));

                assertThat(result.get(0).outNodeId(), is("outNodeId2"));
                assertThat(result.get(0).type(), is(relType));
                assertThat(result.get(0).inNodeId(), is("inNodeId2"));
                assertThat(result.get(0).properties().entrySet(), is(empty()));

                assertThat(result.get(1).outNodeId(), is("outNodeId1"));
                assertThat(result.get(1).type(), is(relType));
                assertThat(result.get(1).inNodeId(), is("inNodeId1"));
                assertThat(result.get(1).properties().entrySet(), is(empty()));

                assertThat(result.get(2).outNodeId(), is("outNodeId3"));
                assertThat(result.get(2).type(), is(relType));
                assertThat(result.get(2).inNodeId(), is("inNodeId3"));
                assertThat(result.get(2).properties().entrySet(), is(empty()));
            }
        }
    }
}
