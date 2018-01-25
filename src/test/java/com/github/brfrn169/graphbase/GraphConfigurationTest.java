package com.github.brfrn169.graphbase;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.github.brfrn169.graphbase.util.Json;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GraphConfigurationTest {
    private static final Json JSON = new Json(JsonInclude.Include.NON_EMPTY);

    @Test public void lombokWithJackson() {
        final String TEST_GRAPH_ID = "test_graph";

        GraphConfiguration graphConfiguration = new GraphConfiguration(TEST_GRAPH_ID);

        String json = JSON.writeValueAsString(graphConfiguration);
        assertEquals("{\"graphId\":\"test_graph\"}", json);

        GraphConfiguration graphConf = JSON.readValue(json, GraphConfiguration.class);
        assertEquals(TEST_GRAPH_ID, graphConf.graphId());
    }
}
