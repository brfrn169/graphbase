package com.github.brfrn169.graphbase;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface GraphStorage extends Closeable {
    void createGraph(String graphId);

    void dropGraph(String graphId);

    void addNode(GraphConfiguration graphConf, String nodeId, String nodeType,
        Map<String, Object> properties);

    void deleteNode(GraphConfiguration graphConf, String nodeId);

    void updateNode(GraphConfiguration graphConf, String nodeId,
        @Nullable Map<String, Object> updateProperties, @Nullable Set<String> deleteKeys);

    Optional<Node> getNode(GraphConfiguration graphConf, String nodeId,
        PropertyProjections propertyProjections);

    boolean nodeExists(GraphConfiguration graphConf, String nodeId);

    void createRelationship(GraphConfiguration graphConf, String outNodeId, String relType,
        String inNodeId, Map<String, Object> properties);

    void deleteRelationship(GraphConfiguration graphConf, String outNodeId, String relType,
        String inNodeId);

    void updateRelationship(GraphConfiguration graphConf, String outNodeId, String relType,
        String inNodeId, @Nullable Map<String, Object> updateProperties,
        @Nullable Set<String> deleteKeys);

    Optional<Relationship> getRelationship(GraphConfiguration graphConf, String outNodeId,
        String relType, String inNodeId, PropertyProjections propertyProjections);

    boolean relationshipExists(GraphConfiguration graphConf, String outNodeId, String relType,
        String inNodeId);
}
