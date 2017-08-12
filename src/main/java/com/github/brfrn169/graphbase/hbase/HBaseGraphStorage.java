package com.github.brfrn169.graphbase.hbase;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.github.brfrn169.graphbase.*;
import com.github.brfrn169.graphbase.exception.NodeAlreadyExistsException;
import com.github.brfrn169.graphbase.exception.NodeNotFoundException;
import com.github.brfrn169.graphbase.exception.RelationshipAlreadyExistsException;
import com.github.brfrn169.graphbase.exception.RelationshipNotFoundException;
import com.github.brfrn169.graphbase.filter.FilterExecutor;
import com.github.brfrn169.graphbase.filter.FilterPredicate;
import com.github.brfrn169.graphbase.filter.FilterPropertyKeysExtractor;
import com.github.brfrn169.graphbase.sort.SortComparator;
import com.github.brfrn169.graphbase.sort.SortPredicate;
import com.github.brfrn169.graphbase.util.Json;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.types.*;
import org.apache.hadoop.hbase.util.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.brfrn169.graphbase.hbase.HBaseClient.addMutations;

public class HBaseGraphStorage implements GraphStorage {

    public static final String TABLE_COMPRESSION_CONF_KEY = "graphbase.hbase.table.compression";

    public static final String TABLE_SPLITS_CONF_KEY = "graphbase.hbase.table.splits";

    private static final Json JSON = new Json(JsonInclude.Include.NON_EMPTY);

    private static final Hash HASH = MurmurHash3.getInstance();

    private static final byte[] ONE_BYTE_ARRAY = new byte[] {0};
    private static final byte[] EXISTENCE_MARKER = ONE_BYTE_ARRAY;

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

    private static final byte[] NODE_SCAN_START_ROW = new byte[] {NODE_ROW_TYPE};
    private static final byte[] NODE_SCAN_STOP_ROW = new byte[] {NODE_ROW_TYPE + 1};
    private static final Pair<byte[], byte[]> NODE_SCAN_ROWS =
        new Pair<>(NODE_SCAN_START_ROW, NODE_SCAN_STOP_ROW);

    // for the relationship schema
    private static final String REL_TABLE_NAME_SUFFIX = "_rel";

    private static final byte[] REL_FAMILY = Bytes.toBytes("r");

    private static final byte REL_ROW_TYPE = (byte) 1;

    private static final Struct REL_STRUCT =
        new StructBuilder().add(new RawByte()).add(new RawInteger())
            .add(new RawStringTerminated("\0")).add(new RawStringTerminated("\0"))
            .add(RawString.ASCENDING).toStruct();

    private static final byte[] REL_QUALIFIER_EXISTENCE_MARKER = HConstants.EMPTY_BYTE_ARRAY;

    private static final Filter REL_EXISTS_FILTER =
        new SingleColumnValueFilter(REL_FAMILY, REL_QUALIFIER_EXISTENCE_MARKER,
            CompareFilter.CompareOp.EQUAL, EXISTENCE_MARKER);

    private static final byte[] REL_SCAN_START_ROW = new byte[] {REL_ROW_TYPE};
    private static final byte[] REL_SCAN_STOP_ROW = new byte[] {REL_ROW_TYPE + 1};
    private static final Pair<byte[], byte[]> REL_SCAN_ROWS =
        new Pair<>(REL_SCAN_START_ROW, REL_SCAN_STOP_ROW);


    private final HBaseClient hbaseClient;
    private final boolean compression;
    private final int splits;

    public HBaseGraphStorage(Configuration conf) {
        hbaseClient = new HBaseClient(conf);
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

        byte[] row = createNodeRow(nodeId);

        if (!nodeExists(graphConf, row)) {
            throw new NodeNotFoundException();
        }

        RowMutations mutation = new RowMutations(row);

        if (updateProperties != null) {
            Put put = new Put(row);
            populatePutWithProperties(put, NODE_FAMILY, updateProperties);

            addMutations(mutation, put);
        }

        if (deleteKeys != null) {
            Delete delete = new Delete(row);
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

        boolean includeAddAt2 = includeAddAt; // too ugly
        return hbaseClient.get(get, getNodeTableName(graphConf.getGraphId()),
            result -> Optional.of(resultToNode(result, includeAddAt2)));
    }

    @Override
    public Stream<Node> getNodes(GraphConfiguration graphConf, @Nullable List<String> nodeTypes,
        @Nullable FilterPredicate filter, @Nullable List<SortPredicate> sorts,
        PropertyProjections propertyProjections) {

        Pair<byte[], byte[]> nodeScanRows = createNodeScanRows();
        byte[] startRow = nodeScanRows.getFirst();
        byte[] stopRow = nodeScanRows.getSecond();

        Scan scan = new Scan(startRow, stopRow);
        if (nodeTypes != null && !nodeTypes.isEmpty()) {
            scan.setFilter(nodeTypesFilter(nodeTypes));
        }

        PropertyProjections propProjections = propertyProjections;
        if (filter != null || sorts != null) {
            Set<String> propertyKeys = new HashSet<>();
            if (filter != null) {
                FilterPropertyKeysExtractor filterPropertyKeysExtractor =
                    new FilterPropertyKeysExtractor(filter);
                propertyKeys.addAll(filterPropertyKeysExtractor.extract());
            }

            if (sorts != null) {
                sorts.forEach(s -> propertyKeys.add(s.getPropertyKey()));
            }
            propProjections = propertyProjections.merge(propertyKeys);
        }

        boolean includeAddAt = true;

        switch (propProjections.getType()) {
            case NOTHING:
                includeAddAt = false;

                scan.addColumn(NODE_FAMILY, NODE_QUALIFIER_TYPE);
                break;
            case PARTIAL:
                includeAddAt =
                    propProjections.getPropertyKeys().contains(GraphbaseConstants.PROPERTY_ADD_AT);

                scan.addColumn(NODE_FAMILY, NODE_QUALIFIER_TYPE);
                propProjections.getPropertyKeys()
                    .forEach(key -> scan.addColumn(NODE_FAMILY, Bytes.toBytes(key)));
                break;
        }

        boolean includeAddAt2 = includeAddAt; // too ugly
        Stream<Node> ret = hbaseClient.scan(scan, getNodeTableName(graphConf.getGraphId()),
            result -> resultToNode(result, includeAddAt2));

        if (filter != null) {
            FilterExecutor filterExecutor = new FilterExecutor(filter);
            ret = ret.filter(filterExecutor::execute);
        }

        if (sorts != null) {
            SortComparator sortComparator = new SortComparator(sorts);
            ret = ret.sorted(sortComparator);
        }

        ret = ret.map(r -> {
            Map<String, Object> properties = propertyProjections.filter(r.getProperties());
            if (r.getProperties() != null) {
                return new Node(r.getId(), r.getType(), properties);
            }
            return r;
        });

        return ret;
    }

    @Override public boolean nodeExists(GraphConfiguration graphConf, String nodeId) {
        return nodeExists(graphConf, createNodeRow(nodeId));
    }

    private boolean nodeExists(GraphConfiguration graphConf, byte[] row) {
        Get get = new Get(row).addColumn(NODE_FAMILY, NODE_QUALIFIER_TYPE);
        return hbaseClient.exists(get, getNodeTableName(graphConf.getGraphId()));
    }

    private byte[] createNodeRow(String nodeId) {
        Object[] values = new Object[] {NODE_ROW_TYPE, HASH.hash(Bytes.toBytes(nodeId)), nodeId};
        PositionedByteRange byteRange =
            new SimplePositionedMutableByteRange(NODE_ROW_STRUCT.encodedLength(values));
        NODE_ROW_STRUCT.encode(byteRange, values);
        return byteRange.getBytes();
    }

    private Pair<byte[], byte[]> createNodeScanRows() {
        return NODE_SCAN_ROWS;
    }

    private Filter nodeTypesFilter(List<String> nodeTypes) {
        List<Filter> filters = nodeTypes.stream().map(
            type -> new SingleColumnValueFilter(NODE_FAMILY, NODE_QUALIFIER_TYPE,
                CompareFilter.CompareOp.EQUAL, Bytes.toBytes(type))).collect(Collectors.toList());
        return new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
    }

    private void populatePutWithProperties(Put put, byte[] family, Map<String, Object> properties) {
        properties.forEach((key, value) -> put
            .addColumn(family, Bytes.toBytes(key), JSON.writeValueAsBytes(value)));
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

        result.getMap().get(family).forEach((key, value) -> {
            if (key.length > 0) {
                ret.put(Bytes.toString(key),
                    JSON.readValue(value.firstEntry().getValue(), Object.class));
            } else {
                // case of NODE_QUALIFIER_TYPE or REL_QUALIFIER_EXISTENCE_MARKER

                if (includeAddAt) {
                    ret.put(GraphbaseConstants.PROPERTY_ADD_AT, value.firstEntry().getKey());
                }
            }
        });

        return ret;
    }

    @Override
    public void createRelationship(GraphConfiguration graphConf, String outNodeId, String relType,
        String inNodeId, Map<String, Object> properties) {
        Put put = new Put(createRelRow(outNodeId, relType, inNodeId));
        put.addColumn(REL_FAMILY, REL_QUALIFIER_EXISTENCE_MARKER, EXISTENCE_MARKER);
        populatePutWithProperties(put, REL_FAMILY, properties);

        if (!hbaseClient
            .checkAndPut(put.getRow(), REL_FAMILY, REL_QUALIFIER_EXISTENCE_MARKER, null, put,
                getRelTableName(graphConf.getGraphId()))) {
            throw new RelationshipAlreadyExistsException();
        }
    }

    @Override
    public void deleteRelationship(GraphConfiguration graphConf, String outNodeId, String relType,
        String inNodeId) {
        Delete delete = new Delete(createRelRow(outNodeId, relType, inNodeId));

        if (!hbaseClient.checkAndDelete(delete.getRow(), REL_FAMILY, REL_QUALIFIER_EXISTENCE_MARKER,
            EXISTENCE_MARKER, delete, getRelTableName(graphConf.getGraphId()))) {
            throw new RelationshipNotFoundException();
        }
    }

    @Override
    public void updateRelationship(GraphConfiguration graphConf, String outNodeId, String relType,
        String inNodeId, @Nullable Map<String, Object> updateProperties,
        @Nullable Set<String> deleteKeys) {

        byte[] row = createRelRow(outNodeId, relType, inNodeId);

        if (!relExists(graphConf, row))
            throw new RelationshipNotFoundException();

        RowMutations mutation = new RowMutations(row);

        if (updateProperties != null && updateProperties.size() > 0) {
            Put put = new Put(row);
            populatePutWithProperties(put, REL_FAMILY, updateProperties);
            addMutations(mutation, put);
        }

        if (deleteKeys != null && deleteKeys.size() > 0) {
            Delete delete = new Delete(row);
            deleteKeys.forEach(key -> delete.addColumns(REL_FAMILY, Bytes.toBytes(key)));
            addMutations(mutation, delete);
        }

        hbaseClient.mutateRow(mutation, getRelTableName(graphConf.getGraphId()));
    }

    @Override
    public Optional<Relationship> getRelationship(GraphConfiguration graphConf, String outNodeId,
        String relType, String inNodeId, PropertyProjections propertyProjections) {

        byte[] row = createRelRow(outNodeId, relType, inNodeId);

        Get get = new Get(row).setFilter(REL_EXISTS_FILTER);

        boolean includeAddAt = true;

        switch (propertyProjections.getType()) {
            case NOTHING:
                includeAddAt = false;

                get.addColumn(REL_FAMILY, REL_QUALIFIER_EXISTENCE_MARKER);
                break;
            case PARTIAL:
                includeAddAt = propertyProjections.getPropertyKeys()
                    .contains(GraphbaseConstants.PROPERTY_ADD_AT);

                get.addColumn(REL_FAMILY, REL_QUALIFIER_EXISTENCE_MARKER);
                propertyProjections.getPropertyKeys()
                    .forEach(key -> get.addColumn(REL_FAMILY, Bytes.toBytes(key)));
                break;
        }

        boolean includeAddAt2 = includeAddAt; // too ugly
        return hbaseClient.get(get, getRelTableName(graphConf.getGraphId()),
            result -> Optional.of(resultToRel(result, includeAddAt2)));
    }

    @Override public Stream<Relationship> getRelationships(GraphConfiguration graphConf,
        @Nullable List<String> relTypes, @Nullable FilterPredicate filter,
        @Nullable List<SortPredicate> sorts, PropertyProjections propertyProjections) {

        Pair<byte[], byte[]> relScanRows = createRelScanRows();
        byte[] startRow = relScanRows.getFirst();
        byte[] stopRow = relScanRows.getSecond();

        Scan scan = new Scan(startRow, stopRow).setFilter(REL_EXISTS_FILTER);

        PropertyProjections propProjections = propertyProjections;
        if (filter != null || sorts != null) {
            Set<String> propertyKeys = new HashSet<>();
            if (filter != null) {
                FilterPropertyKeysExtractor filterPropertyKeysExtractor =
                    new FilterPropertyKeysExtractor(filter);
                propertyKeys.addAll(filterPropertyKeysExtractor.extract());
            }

            if (sorts != null) {
                sorts.forEach(s -> propertyKeys.add(s.getPropertyKey()));
            }
            propProjections = propertyProjections.merge(propertyKeys);
        }

        boolean includeAddAt = true;

        switch (propProjections.getType()) {
            case NOTHING:
                includeAddAt = false;

                scan.addColumn(REL_FAMILY, REL_QUALIFIER_EXISTENCE_MARKER);
                break;
            case PARTIAL:
                includeAddAt =
                    propProjections.getPropertyKeys().contains(GraphbaseConstants.PROPERTY_ADD_AT);

                scan.addColumn(REL_FAMILY, REL_QUALIFIER_EXISTENCE_MARKER);
                propProjections.getPropertyKeys()
                    .forEach(key -> scan.addColumn(REL_FAMILY, Bytes.toBytes(key)));
                break;
        }

        boolean includeAddAt2 = includeAddAt; // too ugly
        Stream<Relationship> ret = hbaseClient.scan(scan, getRelTableName(graphConf.getGraphId()),
            result -> resultToRel(result, includeAddAt2));

        if (relTypes != null && !relTypes.isEmpty()) {
            Set<String> typesSet = new HashSet<>(relTypes);
            ret = ret.filter(r -> typesSet.contains(r.getType()));
        }

        if (filter != null) {
            FilterExecutor filterExecutor = new FilterExecutor(filter);
            ret = ret.filter(filterExecutor::execute);
        }

        if (sorts != null) {
            SortComparator sortComparator = new SortComparator(sorts);
            ret = ret.sorted(sortComparator);
        }

        ret = ret.map(r -> {
            Map<String, Object> properties = propertyProjections.filter(r.getProperties());
            if (r.getProperties() != null) {
                return new Relationship(r.getOutNodeId(), r.getType(), r.getInNodeId(), properties);
            }
            return r;
        });

        return ret;
    }

    @Override public boolean relationshipExists(GraphConfiguration graphConf, String outNodeId,
        String relType, String inNodeId) {
        return relExists(graphConf, createRelRow(outNodeId, relType, inNodeId));
    }

    private boolean relExists(GraphConfiguration graphConf, byte[] row) {
        Get get = new Get(row).addColumn(REL_FAMILY, REL_QUALIFIER_EXISTENCE_MARKER);
        return hbaseClient.exists(get, getRelTableName(graphConf.getGraphId()));
    }

    private byte[] createRelRow(String outNodeId, String relType, String inNodeId) {
        Object[] values =
            new Object[] {REL_ROW_TYPE, HASH.hash(Bytes.toBytes(outNodeId)), outNodeId, relType,
                inNodeId};
        PositionedByteRange byteRange =
            new SimplePositionedMutableByteRange(REL_STRUCT.encodedLength(values));
        REL_STRUCT.encode(byteRange, values);
        return byteRange.getBytes();
    }

    private Pair<byte[], byte[]> createRelScanRows() {
        return REL_SCAN_ROWS;
    }

    private Relationship resultToRel(Result result, boolean includeAddAt) {
        PositionedByteRange byteRange = new SimplePositionedByteRange(result.getRow());
        String outNodeId = (String) REL_STRUCT.decode(byteRange, 2);
        String relationshipType = (String) REL_STRUCT.decode(byteRange, 3);
        String inNodeId = (String) REL_STRUCT.decode(byteRange, 4);
        return new Relationship(outNodeId, relationshipType, inNodeId,
            resultToProperties(result, REL_FAMILY, includeAddAt));
    }
}
