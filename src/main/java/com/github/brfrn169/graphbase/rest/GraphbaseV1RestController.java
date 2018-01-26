package com.github.brfrn169.graphbase.rest;

import com.github.brfrn169.graphbase.GraphConfiguration;
import com.github.brfrn169.graphbase.GraphService;
import com.github.brfrn169.graphbase.GraphStorage;
import com.github.brfrn169.graphbase.Mutation;
import com.github.brfrn169.graphbase.Node;
import com.github.brfrn169.graphbase.PropertyProjections;
import com.github.brfrn169.graphbase.Relationship;
import com.github.brfrn169.graphbase.exception.GraphAlreadyExistsException;
import com.github.brfrn169.graphbase.exception.GraphNotFoundException;
import com.github.brfrn169.graphbase.exception.NodeNotFoundException;
import com.github.brfrn169.graphbase.hbase.HBaseGraphStorage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.github.brfrn169.graphbase.PropertyProjections.Builder.withAllProperties;
import static com.github.brfrn169.graphbase.PropertyProjections.Builder.withProperties;
import static com.github.brfrn169.graphbase.PropertyProjections.Builder.withoutProperties;

@RestController @RequestMapping("/v1/graphs") public class GraphbaseV1RestController {

    private GraphService graphService;
    private GraphStorage graphStorage;

    @PostConstruct public void postConstruct() throws IOException {
        Configuration conf = HBaseConfiguration.create();

        String zookeeperQuorum = System.getProperty(HConstants.ZOOKEEPER_QUORUM);
        if (zookeeperQuorum != null) {
            conf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);
        }

        String zookeeperClientPort = System.getProperty(HConstants.ZOOKEEPER_CLIENT_PORT);
        if (zookeeperClientPort != null) {
            conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.valueOf(zookeeperClientPort));
        }

        String zookeeperZnodeParent = System.getProperty(HConstants.ZOOKEEPER_ZNODE_PARENT);
        if (zookeeperZnodeParent != null) {
            conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zookeeperZnodeParent);
        }

        String tableCompression = System.getProperty(HBaseGraphStorage.TABLE_COMPRESSION_CONF_KEY);
        if (tableCompression != null) {
            conf.setBoolean(HBaseGraphStorage.TABLE_COMPRESSION_CONF_KEY,
                Boolean.valueOf(tableCompression));
        }

        graphStorage = new HBaseGraphStorage(conf);
        graphService = new GraphService(conf, graphStorage);
    }

    @PreDestroy public void preDestroy() throws IOException {
        graphService.close();
        graphStorage.close();
    }

    @RequestMapping(path = "", method = RequestMethod.POST)
    public ResponseEntity<Void> createGraph(@RequestBody GraphConfiguration graphConf) {
        try {
            graphService.createGraph(graphConf);

            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.LOCATION, "/v1/graphs/" + graphConf.graphId());

            return new ResponseEntity<>(headers, HttpStatus.CREATED);
        } catch (GraphAlreadyExistsException e) {
            return new ResponseEntity<>(HttpStatus.CONFLICT);
        }
    }

    @RequestMapping(path = "/{graphId}", method = RequestMethod.DELETE)
    public ResponseEntity<Void> dropGraph(@PathVariable String graphId) {
        try {
            graphService.dropGraph(graphId);
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } catch (GraphNotFoundException e) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @RequestMapping(path = "/{graphId}", method = RequestMethod.GET)
    public ResponseEntity<GraphConfiguration> getGraph(@PathVariable String graphId) {
        return graphService.getGraphConfiguration(graphId)
            .map(graphConf -> new ResponseEntity<>(graphConf, HttpStatus.OK))
            .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    @RequestMapping(path = "/{graphId}", method = RequestMethod.HEAD)
    public ResponseEntity<GraphConfiguration> graphExits(@PathVariable String graphId) {
        if (graphService.graphExists(graphId)) {
            return new ResponseEntity<>(HttpStatus.OK);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @RequestMapping(path = "/{graphId}/nodes", method = RequestMethod.POST)
    public ResponseEntity<Void> addNode(@PathVariable String graphId, @RequestBody Node node) {
        try {
            graphService.addNode(graphId, node.id(), node.type(), node.properties());

            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.LOCATION, "/v1/graphs/" + graphId + "/nodes/" + node.id());

            return new ResponseEntity<>(headers, HttpStatus.CREATED);
        } catch (GraphNotFoundException e) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @RequestMapping(path = "/{graphId}/nodes/{nodeId}", method = RequestMethod.PUT)
    public ResponseEntity<Void> updateNode(@PathVariable String graphId,
        @PathVariable String nodeId, @RequestBody Mutation mutation) {
        try {
            graphService.updateNode(graphId, nodeId, mutation);
            return new ResponseEntity<>(HttpStatus.OK);
        } catch (GraphNotFoundException | NodeNotFoundException e) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @RequestMapping(path = "/{graphId}/nodes/{nodeId}", method = RequestMethod.DELETE)
    public ResponseEntity<Void> deleteNode(@PathVariable String graphId,
        @PathVariable String nodeId) {
        try {
            graphService.deleteNode(graphId, nodeId);
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } catch (GraphNotFoundException | NodeNotFoundException e) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @RequestMapping(path = "/{graphId}/nodes/{nodeId}", method = RequestMethod.GET)
    public ResponseEntity<Node> getNode(@PathVariable String graphId, @PathVariable String nodeId,
        @RequestParam(required = false) String projections) {
        try {
            return graphService.getNode(graphId, nodeId, toPropertyProjections(projections))
                .map(node -> new ResponseEntity<>(node, HttpStatus.OK))
                .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
        } catch (GraphNotFoundException e) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @RequestMapping(path = "/{graphId}/nodes/{nodeId}", method = RequestMethod.HEAD)
    public ResponseEntity<Void> nodeExists(@PathVariable String graphId,
        @PathVariable String nodeId) {
        try {
            boolean exists = graphService.nodeExists(graphId, nodeId);
            if (exists) {
                return new ResponseEntity<>(HttpStatus.OK);
            } else {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }
        } catch (GraphNotFoundException e) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @RequestMapping(path = "/{graphId}/nodes", method = RequestMethod.GET)
    public ResponseEntity<List<Node>> getNodes(@PathVariable String graphId,
        @RequestParam(required = false) String types,
        @RequestParam(required = false) String projections) {
        try {
            List<Node> nodes = graphService
                .getNodes(graphId, toTypes(types), null, null, toPropertyProjections(projections));
            return new ResponseEntity<>(nodes, HttpStatus.OK);
        } catch (GraphNotFoundException e) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @RequestMapping(path = "/{graphId}/relationships", method = RequestMethod.POST)
    public ResponseEntity<Void> addRelationship(@PathVariable String graphId,
        @RequestBody Relationship relationship) {
        try {
            graphService.addRelationship(graphId, relationship.outNodeId(), relationship.type(),
                relationship.inNodeId(), relationship.properties());

            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.LOCATION,
                "/v1/graphs/" + graphId + "/relationships/" + relationship.outNodeId() + "/"
                    + relationship.type() + "/" + relationship.inNodeId());

            return new ResponseEntity<>(headers, HttpStatus.CREATED);
        } catch (GraphNotFoundException e) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @RequestMapping(path = "/{graphId}/relationships/{outNodeId}/{relationshipType}/{inNodeId}", method = RequestMethod.PUT)
    public ResponseEntity<Void> updateRelationship(@PathVariable String graphId,
        @PathVariable String outNodeId, @PathVariable String relationshipType,
        @PathVariable String inNodeId, @RequestBody Mutation mutation) {
        try {
            graphService
                .updateRelationship(graphId, outNodeId, relationshipType, inNodeId, mutation);
            return new ResponseEntity<>(HttpStatus.OK);
        } catch (GraphNotFoundException | NodeNotFoundException e) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @RequestMapping(path = "/{graphId}/relationships/{outNodeId}/{relationshipType}/{inNodeId}", method = RequestMethod.DELETE)
    public ResponseEntity<Void> deleteRelationship(@PathVariable String graphId,
        @PathVariable String outNodeId, @PathVariable String relationshipType,
        @PathVariable String inNodeId) {
        try {
            graphService.deleteRelationship(graphId, outNodeId, relationshipType, inNodeId);
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        } catch (GraphNotFoundException | NodeNotFoundException e) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @RequestMapping(path = "/{graphId}/relationships/{outNodeId}/{relationshipType}/{inNodeId}", method = RequestMethod.GET)
    public ResponseEntity<Relationship> getRelationship(@PathVariable String graphId,
        @PathVariable String outNodeId, @PathVariable String relationshipType,
        @PathVariable String inNodeId, @RequestParam(required = false) String projections) {
        try {
            return graphService.getRelationship(graphId, outNodeId, relationshipType, inNodeId,
                toPropertyProjections(projections))
                .map(rel -> new ResponseEntity<>(rel, HttpStatus.OK))
                .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
        } catch (GraphNotFoundException e) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @RequestMapping(path = "/{graphId}/relationships/{outNodeId}/{relationshipType}/{inNodeId}", method = RequestMethod.HEAD)
    public ResponseEntity<Void> relationshipExists(@PathVariable String graphId,
        @PathVariable String outNodeId, @PathVariable String relationshipType,
        @PathVariable String inNodeId) {
        try {
            boolean exists =
                graphService.relationshipExists(graphId, outNodeId, relationshipType, inNodeId);
            if (exists) {
                return new ResponseEntity<>(HttpStatus.OK);
            } else {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }
        } catch (GraphNotFoundException e) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @RequestMapping(path = "/{graphId}/relationships", method = RequestMethod.GET)
    public ResponseEntity<List<Relationship>> getRelationships(@PathVariable String graphId,
        @RequestParam(required = false) String types,
        @RequestParam(required = false) String projections) {
        try {
            List<Relationship> nodes = graphService
                .getRelationships(graphId, toTypes(types), null, null,
                    toPropertyProjections(projections));
            return new ResponseEntity<>(nodes, HttpStatus.OK);
        } catch (GraphNotFoundException e) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    private static List<String> toTypes(@Nullable String typesStr) {
        if (typesStr == null || typesStr.isEmpty()) {
            return null;
        } else {
            return Arrays.asList(typesStr.split(","));
        }
    }

    private static PropertyProjections toPropertyProjections(
        @Nullable String propertyProjectionsStr) {
        if (propertyProjectionsStr == null) {
            return withAllProperties();
        } else if (propertyProjectionsStr.isEmpty()) {
            return withoutProperties();
        } else {
            Set<String> properties =
                new HashSet<>(Arrays.asList(propertyProjectionsStr.split(",")));
            return withProperties(properties);
        }
    }
}
