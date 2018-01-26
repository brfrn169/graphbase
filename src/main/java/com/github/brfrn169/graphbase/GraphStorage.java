package com.github.brfrn169.graphbase;

import com.github.brfrn169.graphbase.filter.FilterPredicate;
import com.github.brfrn169.graphbase.sort.SortPredicate;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public interface GraphStorage extends Closeable {
    void createGraph(String graphId);

    void dropGraph(String graphId);

    void addNode(GraphConfiguration graphConf, String nodeId, String nodeType,
        Map<String, Object> properties);

    void deleteNode(GraphConfiguration graphConf, String nodeId);

    void updateNode(GraphConfiguration graphConf, String nodeId, Mutation mutation);

    void createRelationship(GraphConfiguration graphConf, String outNodeId, String relType,
        String inNodeId, Map<String, Object> properties);

    void deleteRelationship(GraphConfiguration graphConf, String outNodeId, String relType,
        String inNodeId);

    void updateRelationship(GraphConfiguration graphConf, String outNodeId, String relType,
        String inNodeId, Mutation mutation);

    Optional<Node> getNode(GraphConfiguration graphConf, String nodeId,
        PropertyProjections propertyProjections);

    Optional<Relationship> getRelationship(GraphConfiguration graphConf, String outNodeId,
        String relType, String inNodeId, PropertyProjections propertyProjections);

    boolean nodeExists(GraphConfiguration graphConf, String nodeId);

    boolean relationshipExists(GraphConfiguration graphConf, String outNodeId, String relType,
        String inNodeId);

    Stream<Node> getNodes(GraphConfiguration graphConf, @Nullable List<String> nodeTypes,
        @Nullable FilterPredicate filter, @Nullable List<SortPredicate> sorts,
        PropertyProjections propertyProjections);

    Stream<Relationship> getRelationships(GraphConfiguration graphConf,
        @Nullable List<String> relTypes, @Nullable FilterPredicate filter,
        @Nullable List<SortPredicate> sorts, PropertyProjections propertyProjections);
}
