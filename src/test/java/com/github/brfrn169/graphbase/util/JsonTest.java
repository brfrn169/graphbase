package com.github.brfrn169.graphbase.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.github.brfrn169.graphbase.GraphConfiguration;
import com.github.brfrn169.graphbase.Node;
import com.github.brfrn169.graphbase.Relationship;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@DisplayName("Tests for Json") public class JsonTest {
    private static final Json JSON = new Json(JsonInclude.Include.NON_EMPTY);

    @Test @DisplayName("Test for GraphConfiguration") public void graphConfigurationTest() {
        final String graphId = "graphConfigurationTest";

        GraphConfiguration graphConfiguration = new GraphConfiguration(graphId);

        String json = JSON.writeValueAsString(graphConfiguration);
        assertEquals(String.format("{\"graphId\":\"%s\"}", graphId), json);

        GraphConfiguration graphConf = JSON.readValue(json, GraphConfiguration.class);
        assertEquals(graphId, graphConf.graphId());
    }

    @Test @DisplayName("Test for Node") public void nodeTest() {
        Node origNode = new Node("id", "type", Properties.property("key", "value"));

        String json = JSON.writeValueAsString(origNode);
        assertEquals("{\"id\":\"id\",\"type\":\"type\",\"properties\":{\"key\":\"value\"}}", json);

        Node node = JSON.readValue(json, Node.class);
        assertEquals(origNode.id(), node.id());
        assertEquals(origNode.type(), node.type());
        assertTrue(Properties.equals(origNode.properties(), node.properties()));
    }

    @Test @DisplayName("Test for Relationship") public void relationshipTest() {
        Relationship origRel =
            new Relationship("outNodeId", "type", "inNodeId", Properties.property("key", "value"));

        String json = JSON.writeValueAsString(origRel);
        assertEquals(
            "{\"outNodeId\":\"outNodeId\",\"type\":\"type\",\"inNodeId\":\"inNodeId\",\"properties\":{\"key\":\"value\"}}",
            json);

        Relationship rel = JSON.readValue(json, Relationship.class);
        assertEquals(origRel.outNodeId(), rel.outNodeId());
        assertEquals(origRel.type(), rel.type());
        assertEquals(origRel.inNodeId(), rel.inNodeId());
        assertTrue(Properties.equals(origRel.properties(), rel.properties()));
    }
}
