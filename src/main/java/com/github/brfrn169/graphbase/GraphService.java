package com.github.brfrn169.graphbase;

import com.github.brfrn169.graphbase.exception.GraphAlreadyExistsException;
import com.github.brfrn169.graphbase.exception.GraphNotFoundException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

public class GraphService implements Closeable {

    private static final Log LOG = LogFactory.getLog(GraphService.class);

    private final GraphStorage graphStorage;

    private final GraphCatalogManager graphCatalogManager;

    public GraphService() {
        this(HBaseConfiguration.create());
    }

    public GraphService(Configuration conf) {
        graphStorage = new GraphStorage(conf);
        graphCatalogManager = new GraphCatalogManager(conf);
    }

    @Override public void close() throws IOException {
        try {
            graphStorage.close();
        } catch (IOException e) {
            LOG.error("failed to close graphStorage.", e);
        }
        try {
            graphCatalogManager.close();
        } catch (IOException e) {
            LOG.error("failed to close graphCatalogManager.", e);
        }
    }

    public void createGraph(GraphConfiguration graphConf) {
        if (graphCatalogManager.graphExists(graphConf.getGraphId())) {
            throw new GraphAlreadyExistsException();
        }

        graphCatalogManager.createGraph(graphConf);
        graphStorage.createGraph(graphConf.getGraphId());
    }

    public void dropGraph(String graphId) {
        if (!graphCatalogManager.graphExists(graphId)) {
            throw new GraphNotFoundException();
        }

        graphCatalogManager.dropGraph(graphId);
        graphStorage.dropGraph(graphId);
    }

    public Optional<GraphConfiguration> getGraphConfiguration(String graphId) {
        return graphCatalogManager.getGraphConfiguration(graphId);
    }
}
