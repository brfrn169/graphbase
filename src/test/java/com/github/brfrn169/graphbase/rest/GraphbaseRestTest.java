package com.github.brfrn169.graphbase.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.github.brfrn169.graphbase.GraphConfiguration;
import com.github.brfrn169.graphbase.Node;
import com.github.brfrn169.graphbase.Relationship;
import com.github.brfrn169.graphbase.hbase.HBaseGraphStorage;
import com.github.brfrn169.graphbase.util.Json;
import com.github.brfrn169.graphbase.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT) @ExtendWith(SpringExtension.class)
public class GraphbaseRestTest {

    private static final Json JSON = new Json(JsonInclude.Include.NON_EMPTY);
    private static HBaseTestingUtility testUtil;

    @Autowired private TestRestTemplate restTemplate;

    @BeforeAll public static void beforeAll() throws Exception {
        testUtil = new HBaseTestingUtility();
        testUtil.startMiniCluster();

        Configuration conf = testUtil.getConfiguration();
        System.setProperty(HConstants.ZOOKEEPER_QUORUM, conf.get(HConstants.ZOOKEEPER_QUORUM));
        System.setProperty(HConstants.ZOOKEEPER_CLIENT_PORT,
            conf.get(HConstants.ZOOKEEPER_CLIENT_PORT));
        System.setProperty(HConstants.ZOOKEEPER_ZNODE_PARENT,
            conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
        System.setProperty(HBaseGraphStorage.TABLE_COMPRESSION_CONF_KEY, "false");
    }

    @AfterAll public static void afterAll() throws Exception {
        testUtil.shutdownMiniCluster();
    }

    private void createGraph(String graphId) {
        GraphConfiguration graphConf = new GraphConfiguration(graphId);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> requestEntry =
            new HttpEntity<>(JSON.writeValueAsString(graphConf), headers);

        ResponseEntity<Void> responseEntity =
            restTemplate.postForEntity("/v1/graphs", requestEntry, Void.class);

        assertThat(responseEntity.getStatusCode(), is(HttpStatus.CREATED));
    }

    @Test @DisplayName("Test related to graph catalog") public void graphCatalogTest() {
        final String graphId = "graphCatalogTest";
        createGraph(graphId);

        String url = "/v1/graphs/" + graphId;
        {
            ResponseEntity<GraphConfiguration> responseEntity =
                restTemplate.getForEntity(url, GraphConfiguration.class);

            assertThat(responseEntity.getStatusCode(), is(HttpStatus.OK));

            GraphConfiguration body = responseEntity.getBody();
            assertThat(body, not(nullValue()));
            if (body != null) {
                assertThat(body.graphId(), is(graphId));
            }
        }
        {
            ResponseEntity<Void> responseEntity =
                restTemplate.exchange(url, HttpMethod.DELETE, null, Void.class);
            assertThat(responseEntity.getStatusCode(), is(HttpStatus.NO_CONTENT));
        }
    }

    private void addNode(String graphId, Node node) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> requestEntry = new HttpEntity<>(JSON.writeValueAsString(node), headers);

        ResponseEntity<Void> responseEntity = restTemplate
            .postForEntity("/v1/graphs/" + graphId + "/nodes", requestEntry, Void.class);

        assertThat(responseEntity.getStatusCode(), is(HttpStatus.CREATED));
    }

    private void getNode(String graphId, Node node) {
        ResponseEntity<Node> responseEntity =
            restTemplate.getForEntity("/v1/graphs/" + graphId + "/nodes/" + node.id(), Node.class);

        assertThat(responseEntity.getStatusCode(), is(HttpStatus.OK));

        Node body = responseEntity.getBody();
        assertThat(body, not(nullValue()));
        if (body != null) {
            assertThat(body.id(), is(node.id()));
            assertThat(body.type(), is(node.type()));
            assertTrue(Properties.equals(body.properties(), node.properties(), true));
        }
    }

    @Test @DisplayName("Test related to nodes") public void nodesTest() {
        final String graphId = "nodesTest";
        createGraph(graphId);

        Node node1 = new Node("id1", "type1", Properties.property("key1", "value1"));
        Node node2 = new Node("id2", "type2", Properties.property("key2", "value2"));
        Node node3 = new Node("id3", "type3", Properties.property("key3", "value3"));

        addNode(graphId, node1);
        addNode(graphId, node2);
        addNode(graphId, node3);

        getNode(graphId, node1);
        getNode(graphId, node2);
        getNode(graphId, node3);

        {
            Map<Node, Node> nodesMap = new HashMap<>();
            nodesMap.put(node1, node1);
            nodesMap.put(node2, node2);
            nodesMap.put(node3, node3);

            ResponseEntity<List<Node>> responseEntity = restTemplate
                .exchange("/v1/graphs/" + graphId + "/nodes", HttpMethod.GET, null,
                    new ParameterizedTypeReference<List<Node>>() {
                    });

            List<Node> body = responseEntity.getBody();
            assertThat(body, not(nullValue()));
            if (body != null) {
                body.forEach(node -> {
                    Node n = nodesMap.remove(node);
                    assertThat(n, not(nullValue()));
                    assertThat(node.type(), is(n.type()));
                    assertTrue(Properties.equals(node.properties(), n.properties(), true));
                });
                assertThat(nodesMap.entrySet(), is(empty()));
            }
        }
    }

    private void addRelationship(String graphId, Relationship rel) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> requestEntry = new HttpEntity<>(JSON.writeValueAsString(rel), headers);

        ResponseEntity<Void> responseEntity = restTemplate
            .postForEntity("/v1/graphs/" + graphId + "/relationships", requestEntry, Void.class);

        assertThat(responseEntity.getStatusCode(), is(HttpStatus.CREATED));
    }

    private void getRelationship(String graphId, Relationship rel) {
        ResponseEntity<Relationship> responseEntity = restTemplate.getForEntity(
            "/v1/graphs/" + graphId + "/relationships/" + rel.outNodeId() + "/" + rel.type() + "/"
                + rel.inNodeId(), Relationship.class);

        assertThat(responseEntity.getStatusCode(), is(HttpStatus.OK));

        Relationship body = responseEntity.getBody();
        assertThat(body, not(nullValue()));
        if (body != null) {
            assertThat(body.outNodeId(), is(rel.outNodeId()));
            assertThat(body.type(), is(rel.type()));
            assertThat(body.inNodeId(), is(rel.inNodeId()));
            assertTrue(Properties.equals(body.properties(), rel.properties(), true));
        }
    }

    @Test @DisplayName("Test related to relationships") public void relationshipsTest() {
        final String graphId = "relationshipsTest";
        createGraph(graphId);

        Relationship rel1 = new Relationship("outNodeId1", "type1", "inNodeId1",
            Properties.property("key1", "value1"));
        Relationship rel2 = new Relationship("outNodeId2", "type2", "inNodeId2",
            Properties.property("key2", "value2"));
        Relationship rel3 = new Relationship("outNodeId3", "type3", "inNodeId3",
            Properties.property("key3", "value3"));

        addRelationship(graphId, rel1);
        addRelationship(graphId, rel2);
        addRelationship(graphId, rel3);

        getRelationship(graphId, rel1);
        getRelationship(graphId, rel2);
        getRelationship(graphId, rel3);

        {
            Map<Relationship, Relationship> relsMap = new HashMap<>();
            relsMap.put(rel1, rel1);
            relsMap.put(rel2, rel2);
            relsMap.put(rel3, rel3);

            ResponseEntity<List<Relationship>> responseEntity = restTemplate
                .exchange("/v1/graphs/" + graphId + "/relationships", HttpMethod.GET, null,
                    new ParameterizedTypeReference<List<Relationship>>() {
                    });

            List<Relationship> body = responseEntity.getBody();
            assertThat(body, not(nullValue()));
            if (body != null) {
                body.forEach(rel -> {
                    Relationship r = relsMap.remove(rel);
                    assertThat(r, not(nullValue()));

                    assertThat(r.type(), is(rel.type()));
                    assertThat(r.inNodeId(), is(rel.inNodeId()));
                    assertTrue(Properties.equals(r.properties(), rel.properties(), true));
                });
                assertThat(relsMap.entrySet(), is(empty()));
            }
        }
    }
}
