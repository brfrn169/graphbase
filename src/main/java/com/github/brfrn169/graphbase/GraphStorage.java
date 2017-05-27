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
}
