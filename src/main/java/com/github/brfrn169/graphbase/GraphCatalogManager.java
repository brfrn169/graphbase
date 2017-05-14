package com.github.brfrn169.graphbase;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.github.brfrn169.graphbase.exception.GraphbaseException;
import com.github.brfrn169.graphbase.util.Json;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class GraphCatalogManager implements Closeable {

    public static final String ZOOKEEPER_BASE_PATH_CONF_KEY =
        "graphbase.hbase.catalog.zookeeper.base.path";

    public static final String ZOOKEEPER_QUORUM_CONF_KEY =
        "graphbase.hbase.catalog.zookeeper.quorum";

    public static final String ZOOKEEPER_SESSION_TIMEOUT_CONF_KEY =
        "graphbase.hbase.catalog.zookeeper.session.timeout";

    public static final String ZOOKEEPER_CONNECTION_TIMEOUT_CONF_KEY =
        "graphbase.hbase.catalog.zookeeper.connection.timeout";

    public static final String ZOOKEEPER_RETRY_CONF_KEY = "graphbase.hbase.catalog.zookeeper.retry";

    private final String catalogBasePath;

    private final ConcurrentMap<String, GraphConfiguration> graphConfMap =
        new ConcurrentHashMap<>();

    private final CuratorFramework client;

    private final Json json;

    public GraphCatalogManager(Configuration conf) {
        json = new Json(JsonInclude.Include.NON_EMPTY);

        catalogBasePath = conf.get(ZOOKEEPER_BASE_PATH_CONF_KEY, "/graphbase/catalog");

        String quorum =
            conf.get(ZOOKEEPER_QUORUM_CONF_KEY, conf.get(HConstants.ZOOKEEPER_QUORUM)) + ":" + conf
                .getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181);

        int sessionTimeout = conf.getInt(ZOOKEEPER_SESSION_TIMEOUT_CONF_KEY,
            conf.getInt(HConstants.ZK_SESSION_TIMEOUT, 20000));
        int connectionTimeout = conf.getInt(ZOOKEEPER_CONNECTION_TIMEOUT_CONF_KEY, 10000);
        int retryCount = conf.getInt(ZOOKEEPER_RETRY_CONF_KEY, 10);

        client =
            CuratorFrameworkFactory.builder().connectString(quorum).sessionTimeoutMs(sessionTimeout)
                .retryPolicy(new RetryNTimes(retryCount, 100))
                .connectionTimeoutMs(connectionTimeout).build();
        client.start();

        try {
            client.create().creatingParentsIfNeeded().forPath(catalogBasePath);
        } catch (final KeeperException.NodeExistsException ignored) {
        } catch (final Exception e) {
            throw new GraphbaseException("an error occurred during creating a base path.", e);
        }

        setCatalogWatcher();
    }

    private synchronized void setCatalogWatcher() {
        try {
            List<String> children = client.getChildren().usingWatcher((Watcher) event -> {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    setCatalogWatcher();
                }
            }).forPath(catalogBasePath);

            Set<String> removedGraphIds = new HashSet<>(graphConfMap.keySet());

            for (String graphId : children) {
                if (graphConfMap.containsKey(graphId)) {
                    removedGraphIds.remove(graphId);
                } else {
                    Optional<GraphConfiguration> graphConf =
                        getGraphGraphConfigurationInternal(graphId);
                    graphConf.ifPresent(conf -> graphConfMap.put(graphId, conf));
                }
            }

            removedGraphIds.forEach(graphConfMap::remove);
        } catch (Exception e) {
            throw new GraphbaseException("an error occurred during setting a watcher.", e);
        }
    }

    private Optional<GraphConfiguration> getGraphGraphConfigurationInternal(String graphId) {
        try {
            byte[] result = client.getData().usingWatcher((Watcher) event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                    getGraphGraphConfigurationInternal(graphId)
                        .ifPresent(graphConf -> graphConfMap.put(graphId, graphConf));
                }
            }).forPath(catalogBasePath + "/" + graphId);
            return Optional.of(json.readValue(result, GraphConfiguration.class));
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        } catch (Exception e) {
            throw new GraphbaseException("an error occurred during getting graph configuration.",
                e);
        }
    }

    public boolean graphExists(String graphId) {
        return graphConfMap.containsKey(graphId);
    }

    public Optional<GraphConfiguration> getGraphConfiguration(String graphId) {
        return Optional.ofNullable(graphConfMap.get(graphId));
    }

    public void createGraph(GraphConfiguration graphConf) {
        if (!graphConfMap.containsKey(graphConf.getGraphId())) {
            try {
                client.create().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(catalogBasePath + "/" + graphConf.getGraphId(),
                        json.writeValueAsBytes(graphConf));
            } catch (final KeeperException.NodeExistsException ignored) {
            } catch (final Exception e) {
                throw new GraphbaseException(
                    "an error occurred during setting graph configuration.", e);
            }
        }

        try {
            client.setData().forPath(catalogBasePath + "/" + graphConf.getGraphId(),
                json.writeValueAsBytes(graphConf));
        } catch (final Exception e) {
            throw new GraphbaseException("an error occurred during setting graph configuration.",
                e);
        }
    }

    public void dropGraph(String graphId) {
        try {
            client.delete().forPath(catalogBasePath + "/" + graphId);
        } catch (final KeeperException.NoNodeException ignored) {
        } catch (final Exception e) {
            throw new GraphbaseException("an error occurred during deleting graph configuration.",
                e);
        }
    }

    @Override public void close() throws IOException {
        client.close();
    }
}
