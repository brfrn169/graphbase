package com.github.brfrn169.graphbase;

import com.github.brfrn169.graphbase.exception.GraphNotFoundException;
import com.github.brfrn169.graphbase.filter.FilterPredicate;
import com.github.brfrn169.graphbase.sort.SortPredicate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GraphService implements Closeable {

    private static final Log LOG = LogFactory.getLog(GraphService.class);

    private final GraphCatalogManager graphCatalogManager;
    private final GraphStorage graphStorage;

    public GraphService(Configuration conf, GraphStorage graphStorage) {
        graphCatalogManager = new GraphCatalogManager(conf);
        this.graphStorage = graphStorage;
    }

    @Override public void close() throws IOException {
        try {
            graphCatalogManager.close();
        } catch (IOException e) {
            LOG.error("failed to close graphCatalogManager.", e);
        }
    }

    public void createGraph(GraphConfiguration graphConf) {
        graphCatalogManager.createGraph(graphConf);
        graphStorage.createGraph(graphConf.getGraphId());
    }

    public void dropGraph(String graphId) {
        graphCatalogManager.dropGraph(graphId);
        graphStorage.dropGraph(graphId);
    }

    public Optional<GraphConfiguration> getGraphConfiguration(String graphId) {
        return graphCatalogManager.getGraphConfiguration(graphId);
    }

    public void addNode(String graphId, String nodeId, String nodeType,
        Map<String, Object> properties) {
        GraphConfiguration graphConf = graphCatalogManager.getGraphConfiguration(graphId)
            .orElseThrow(GraphNotFoundException::new);

        graphStorage.addNode(graphConf, nodeId, nodeType, properties);
    }

    public void deleteNode(String graphId, String nodeId) {
        GraphConfiguration graphConf = graphCatalogManager.getGraphConfiguration(graphId)
            .orElseThrow(GraphNotFoundException::new);

        graphStorage.deleteNode(graphConf, nodeId);
    }

    public void updateNode(String graphId, String nodeId,
        @Nullable Map<String, Object> updateProperties, @Nullable Set<String> deleteKeys) {
        GraphConfiguration graphConf = graphCatalogManager.getGraphConfiguration(graphId)
            .orElseThrow(GraphNotFoundException::new);

        graphStorage.updateNode(graphConf, nodeId, updateProperties, deleteKeys);
    }

    public void addRelationship(String graphId, String outNodeId, String relType, String inNodeId,
        Map<String, Object> properties) {
        GraphConfiguration graphConf = graphCatalogManager.getGraphConfiguration(graphId)
            .orElseThrow(GraphNotFoundException::new);

        graphStorage.createRelationship(graphConf, outNodeId, relType, inNodeId, properties);
    }

    public void deleteRelationship(String graphId, String outNodeId, String relType,
        String inNodeId) {
        GraphConfiguration graphConf = graphCatalogManager.getGraphConfiguration(graphId)
            .orElseThrow(GraphNotFoundException::new);

        graphStorage.deleteRelationship(graphConf, outNodeId, relType, inNodeId);
    }

    public void updateRelationship(String graphId, String outNodeId, String relType,
        String inNodeId, @Nullable Map<String, Object> updateProperties,
        @Nullable Set<String> deleteKeys) {
        GraphConfiguration graphConf = graphCatalogManager.getGraphConfiguration(graphId)
            .orElseThrow(GraphNotFoundException::new);

        graphStorage.updateRelationship(graphConf, outNodeId, relType, inNodeId, updateProperties,
            deleteKeys);
    }

    public Optional<Node> getNode(String graphId, String nodeId,
        PropertyProjections propertyProjections) {
        GraphConfiguration graphConf = graphCatalogManager.getGraphConfiguration(graphId)
            .orElseThrow(GraphNotFoundException::new);

        return graphStorage.getNode(graphConf, nodeId, propertyProjections);
    }

    public Optional<Relationship> getRelationship(String graphId, String outNodeId, String relType,
        String inNodeId, PropertyProjections propertyProjections) {
        GraphConfiguration graphConf = graphCatalogManager.getGraphConfiguration(graphId)
            .orElseThrow(GraphNotFoundException::new);

        return graphStorage
            .getRelationship(graphConf, outNodeId, relType, inNodeId, propertyProjections);
    }

    public boolean nodeExists(String graphId, String nodeId) {
        GraphConfiguration graphConf = graphCatalogManager.getGraphConfiguration(graphId)
            .orElseThrow(GraphNotFoundException::new);
        return graphStorage.nodeExists(graphConf, nodeId);
    }

    public boolean relationshipExists(String graphId, String outNodeId, String relType,
        String inNodeId) {
        GraphConfiguration graphConf = graphCatalogManager.getGraphConfiguration(graphId)
            .orElseThrow(GraphNotFoundException::new);

        return graphStorage.relationshipExists(graphConf, outNodeId, relType, inNodeId);
    }

    public List<Node> getNodes(String graphId, @Nullable List<String> nodeTypes,
        @Nullable FilterPredicate filter, @Nullable List<SortPredicate> sorts,
        PropertyProjections propertyProjections) {
        GraphConfiguration graphConf = graphCatalogManager.getGraphConfiguration(graphId)
            .orElseThrow(GraphNotFoundException::new);

        try (Stream<Node> nodes = graphStorage
            .getNodes(graphConf, nodeTypes, filter, sorts, propertyProjections)) {
            return nodes.collect(Collectors.toList());
        }
    }

    public List<Relationship> getRelationships(String graphId, @Nullable List<String> relTypes,
        @Nullable FilterPredicate filter, @Nullable List<SortPredicate> sorts,
        PropertyProjections propertyProjections) {
        GraphConfiguration graphConf = graphCatalogManager.getGraphConfiguration(graphId)
            .orElseThrow(GraphNotFoundException::new);

        try (Stream<Relationship> rels = graphStorage
            .getRelationships(graphConf, relTypes, filter, sorts, propertyProjections)) {
            return rels.collect(Collectors.toList());
        }
    }
}
