package com.github.brfrn169.graphbase.hbase;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.github.brfrn169.graphbase.*;
import com.github.brfrn169.graphbase.exception.NodeAlreadyExistsException;
import com.github.brfrn169.graphbase.exception.NodeNotFoundException;
import com.github.brfrn169.graphbase.util.Json;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.types.*;
import org.apache.hadoop.hbase.util.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

import static com.github.brfrn169.graphbase.hbase.HBaseClientWrapper.addMutations;

public class HBaseGraphStorage implements GraphStorage {

    public static final String TABLE_COMPRESSION_CONF_KEY = "graphbase.hbase.table.compression";

    public static final String TABLE_SPLITS_CONF_KEY = "graphbase.hbase.table.splits";

    private static final Json JSON = new Json(JsonInclude.Include.NON_EMPTY);

    private static final Hash HASH = MurmurHash3.getInstance();

    private static final byte[] ONE_BYTE_ARRAY = new byte[] {0};


    private static final String NAMESPACE = "graphbase";

    // for the node schema
    private static final String NODE_TABLE_NAME_SUFFIX = "_node";

    private static final byte[] NODE_FAMILY = Bytes.toBytes("n");

    private static final byte NODE_ROW_TYPE = (byte) 1;

    private static final Struct NODE_ROW_STRUCT =
        new StructBuilder().add(new RawByte()).add(new RawInteger()).add(RawString.ASCENDING)
            .toStruct();

    private static final byte[] NODE_QUALIFIER_TYPE = HConstants.EMPTY_BYTE_ARRAY;

    private static final Filter NODE_EXISTS_FILTER =
        new SingleColumnValueFilter(NODE_FAMILY, NODE_QUALIFIER_TYPE,
            CompareFilter.CompareOp.GREATER_OR_EQUAL, ONE_BYTE_ARRAY);


    // for the relationship schema
    private static final String REL_TABLE_NAME_SUFFIX = "_rel";
    private static final byte[] REL_FAMILY = Bytes.toBytes("r");


    private final HBaseClientWrapper hbaseClient;
    private final boolean compression;
    private final int splits;

    public HBaseGraphStorage(Configuration conf) {
        hbaseClient = new HBaseClientWrapper(conf);
        compression = conf.getBoolean(TABLE_COMPRESSION_CONF_KEY, true);
        splits = conf.getInt(TABLE_SPLITS_CONF_KEY, 1);

        ensureCreatingNamespace();
    }

    private void ensureCreatingNamespace() {
        hbaseClient.createNamespace(NAMESPACE);
    }

    @Override public void close() throws IOException {
        hbaseClient.close();
    }

    @Override public void createGraph(String graphId) {
        // create a node table
        {
            byte[][] splitKeys = new byte[splits][];
            splitKeys[0] = new byte[] {2};

            for (int i = 0; i < splits - 1; i++) {
                splitKeys[i + 1] = new byte[] {2, (byte) (256 / splits * (i + 1))};
            }

            hbaseClient.createTable(getNodeHTableDescriptor(graphId, compression), splitKeys);
        }

        // create a relationship table
        {
            byte[][] splitKeys = new byte[splits][];
            splitKeys[0] = new byte[] {2};

            for (int i = 0; i < splits - 1; i++) {
                splitKeys[i + 1] = new byte[] {2, (byte) (256 / splits * (i + 1))};
            }

            hbaseClient.createTable(getRelHTableDescriptor(graphId, compression), splitKeys);
        }
    }

    @Override public void dropGraph(String graphId) {
        Arrays.asList(getNodeTableName(graphId), getRelTableName(graphId))
            .forEach(hbaseClient::deleteTable);
    }

    private TableName getNodeTableName(String graphId) {
        return TableName.valueOf(NAMESPACE, graphId + NODE_TABLE_NAME_SUFFIX);
    }

    private HTableDescriptor getNodeHTableDescriptor(String graphId, boolean compression) {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(getNodeTableName(graphId));

        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(NODE_FAMILY);
        hColumnDescriptor.setMaxVersions(Integer.MAX_VALUE);
        hColumnDescriptor.setKeepDeletedCells(KeepDeletedCells.TRUE);
        if (compression) {
            hColumnDescriptor.setCompressionType(Compression.Algorithm.LZ4);
        }
        hColumnDescriptor.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        hTableDescriptor.addFamily(hColumnDescriptor);
        return hTableDescriptor;
    }

    private TableName getRelTableName(String graphId) {
        return TableName.valueOf(NAMESPACE, graphId + REL_TABLE_NAME_SUFFIX);
    }

    private HTableDescriptor getRelHTableDescriptor(String graphId, boolean compression) {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(getRelTableName(graphId));

        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(REL_FAMILY);
        hColumnDescriptor.setMaxVersions(Integer.MAX_VALUE);
        hColumnDescriptor.setKeepDeletedCells(KeepDeletedCells.TRUE);
        if (compression) {
            hColumnDescriptor.setCompressionType(Compression.Algorithm.LZ4);
        }
        hColumnDescriptor.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        hTableDescriptor.addFamily(hColumnDescriptor);
        return hTableDescriptor;
    }

    @Override public void addNode(GraphConfiguration graphConf, String nodeId, String nodeType,
        Map<String, Object> properties) {

        byte[] row = createNodeRow(nodeId);
        Put put = new Put(row);

        put.addColumn(NODE_FAMILY, NODE_QUALIFIER_TYPE, Bytes.toBytes(nodeType));

        populatePutWithProperties(put, NODE_FAMILY, properties);

        if (!hbaseClient.checkAndPut(put.getRow(), NODE_FAMILY, NODE_QUALIFIER_TYPE, null, put,
            getNodeTableName(graphConf.getGraphId()))) {
            throw new NodeAlreadyExistsException();
        }
    }

    @Override public void deleteNode(GraphConfiguration graphConf, String nodeId) {
        Delete delete = new Delete(createNodeRow(nodeId));

        if (!hbaseClient.checkAndDelete(delete.getRow(), NODE_FAMILY, NODE_QUALIFIER_TYPE,
            CompareFilter.CompareOp.LESS, ONE_BYTE_ARRAY, delete,
            getNodeTableName(graphConf.getGraphId()))) {

            throw new NodeNotFoundException();
        }
    }

    @Override public void updateNode(GraphConfiguration graphConf, String nodeId,
        @Nullable Map<String, Object> updateProperties, @Nullable Set<String> deleteKeys) {

        byte[] nodeRow = createNodeRow(nodeId);

        // check if the node exists
        if (!nodeExists(graphConf, nodeRow)) {
            throw new NodeNotFoundException();
        }

        // mutate the node's properties.
        RowMutations mutation = new RowMutations(nodeRow);

        if (updateProperties != null) {
            Put put = new Put(nodeRow);
            populatePutWithProperties(put, NODE_FAMILY, updateProperties);

            addMutations(mutation, put);
        }

        if (deleteKeys != null) {
            Delete delete = new Delete(nodeRow);
            deleteKeys.forEach(key -> delete.addColumns(NODE_FAMILY, Bytes.toBytes(key)));

            addMutations(mutation, delete);
        }

        hbaseClient.mutateRow(mutation, getNodeTableName(graphConf.getGraphId()));
    }

    @Override public Optional<Node> getNode(GraphConfiguration graphConf, String nodeId,
        PropertyProjections propertyProjections) {

        byte[] nodeRow = createNodeRow(nodeId);
        Get get = new Get(nodeRow).setFilter(NODE_EXISTS_FILTER);

        boolean includeAddAt = true;

        switch (propertyProjections.getType()) {
            case NOTHING:
                includeAddAt = false;

                get.addColumn(NODE_FAMILY, NODE_QUALIFIER_TYPE);
                break;
            case PARTIAL:
                includeAddAt = propertyProjections.getPropertyKeys()
                    .contains(GraphbaseConstants.PROPERTY_ADD_AT);

                get.addColumn(NODE_FAMILY, NODE_QUALIFIER_TYPE);
                propertyProjections.getPropertyKeys()
                    .forEach(key -> get.addColumn(NODE_FAMILY, Bytes.toBytes(key)));
                break;
        }

        Result result = hbaseClient.get(get, getNodeTableName(graphConf.getGraphId()));
        if (result.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(resultToNode(result, includeAddAt));
    }

    private boolean nodeExists(GraphConfiguration graphConf, byte[] nowRow) {
        Get get = new Get(nowRow).addColumn(NODE_FAMILY, NODE_QUALIFIER_TYPE);
        return hbaseClient.exists(get, getNodeTableName(graphConf.getGraphId()));
    }

    private byte[] createNodeRow(String nodeId) {
        Object[] values = new Object[] {NODE_ROW_TYPE, HASH.hash(Bytes.toBytes(nodeId)), nodeId};
        PositionedByteRange byteRange =
            new SimplePositionedMutableByteRange(NODE_ROW_STRUCT.encodedLength(values));
        NODE_ROW_STRUCT.encode(byteRange, values);
        return byteRange.getBytes();
    }

    private void populatePutWithProperties(Put put, byte[] family, Map<String, Object> properties) {
        properties.entrySet().forEach(entry -> put.addColumn(family, Bytes.toBytes(entry.getKey()),
            JSON.writeValueAsBytes(entry.getValue())));
    }

    private Node resultToNode(Result result, boolean includeAddAt) {
        PositionedByteRange byteRange = new SimplePositionedByteRange(result.getRow());
        String nodeId = (String) NODE_ROW_STRUCT.decode(byteRange, 2);
        String nodeType = Bytes.toString(result.getValue(NODE_FAMILY, NODE_QUALIFIER_TYPE));
        return new Node(nodeId, nodeType, resultToProperties(result, NODE_FAMILY, includeAddAt));
    }

    private Map<String, Object> resultToProperties(Result result, byte[] family,
        boolean includeAddAt) {
        Map<String, Object> ret = new HashMap<>();

        result.getMap().get(family).entrySet().forEach(entry -> {
            if (entry.getKey().length > 0) {
                ret.put(Bytes.toString(entry.getKey()),
                    JSON.readValue(entry.getValue().firstEntry().getValue(), Object.class));
            } else {
                // NODE_QUALIFIER_TYPE

                if (includeAddAt) {
                    ret.put(GraphbaseConstants.PROPERTY_ADD_AT,
                        entry.getValue().firstEntry().getKey());
                }
            }
        });

        return ret;
    }
}
