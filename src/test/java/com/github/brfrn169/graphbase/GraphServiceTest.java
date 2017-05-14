package com.github.brfrn169.graphbase;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Optional;

import static org.junit.Assert.*;

@RunWith(Enclosed.class) public class GraphServiceTest {

    private static HBaseTestingUtility testUtil = new HBaseTestingUtility();
    private static GraphService graphService;

    @BeforeClass public static void beforeClass() throws Exception {
        testUtil.getConfiguration().setBoolean(GraphStorage.TABLE_COMPRESSION_CONF_KEY, false);

        testUtil.startMiniCluster();
        graphService = new GraphService(testUtil.getConfiguration());
    }

    @AfterClass public static void afterClass() throws Exception {
        testUtil.shutdownMiniCluster();
    }

    public static class GraphCatalogTest {
        @Test public void createAndDropGraph() {
            final String TEST_GRAPH_ID = "test_graph";

            graphService.createGraph(new GraphConfiguration(TEST_GRAPH_ID));

            Optional<GraphConfiguration> graphConfiguration =
                graphService.getGraphConfiguration(TEST_GRAPH_ID);
            assertTrue(graphConfiguration.isPresent());
            graphConfiguration
                .ifPresent(graphConf -> assertEquals(TEST_GRAPH_ID, graphConf.getGraphId()));

            graphService.dropGraph(TEST_GRAPH_ID);

            graphConfiguration = graphService.getGraphConfiguration(TEST_GRAPH_ID);
            assertFalse(graphConfiguration.isPresent());
        }
    }
}
