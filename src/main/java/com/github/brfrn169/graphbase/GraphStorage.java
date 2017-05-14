package com.github.brfrn169.graphbase;

import com.github.brfrn169.graphbase.schema.NodeSchema;
import com.github.brfrn169.graphbase.schema.RelationshipSchema;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

public class GraphStorage implements Closeable {

    public static final String TABLE_COMPRESSION_CONF_KEY = "graphbase.hbase.table.compression";

    public static final String TABLE_SPLITS_CONF_KEY = "graphbase.hbase.table.splits";

    private final HBaseClientWrapper hbaseClient;

    private final boolean compression;

    private final int splits;

    public GraphStorage(Configuration conf) {
        hbaseClient = new HBaseClientWrapper(conf);
        compression = conf.getBoolean(TABLE_COMPRESSION_CONF_KEY, true);
        splits = conf.getInt(TABLE_SPLITS_CONF_KEY, 1);

        ensureCreatingNamespace();
    }

    private void ensureCreatingNamespace() {
        hbaseClient.createNamespace(GraphbaseConstants.NAMESPACE);
    }

    @Override public void close() throws IOException {
        hbaseClient.close();
    }

    public void createGraph(String graphId) {
        // create a node table
        {
            byte[][] splitKeys = new byte[splits][];
            splitKeys[0] = new byte[] {2};

            for (int i = 0; i < splits - 1; i++) {
                splitKeys[i + 1] = new byte[] {2, (byte) (256 / splits * (i + 1))};
            }

            hbaseClient
                .createTable(NodeSchema.getHTableDescriptor(graphId, compression), splitKeys);
        }

        // create a relationship table
        {
            byte[][] splitKeys = new byte[splits][];
            splitKeys[0] = new byte[] {2};

            for (int i = 0; i < splits - 1; i++) {
                splitKeys[i + 1] = new byte[] {2, (byte) (256 / splits * (i + 1))};
            }

            hbaseClient.createTable(RelationshipSchema.getHTableDescriptor(graphId, compression),
                splitKeys);
        }
    }

    public void dropGraph(String graphId) {
        Arrays.asList(NodeSchema.getTableName(graphId), RelationshipSchema.getTableName(graphId))
            .forEach(hbaseClient::dropTable);
    }
}
