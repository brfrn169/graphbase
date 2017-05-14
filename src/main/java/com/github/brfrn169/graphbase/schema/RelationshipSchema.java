package com.github.brfrn169.graphbase.schema;


import com.github.brfrn169.graphbase.GraphbaseConstants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

public final class RelationshipSchema {

    private static final String TABLE_NAME_SUFFIX = "_rel";

    private static final byte[] FAMILY = Bytes.toBytes("r");

    private RelationshipSchema() {
    }

    public static TableName getTableName(String graphId) {
        return TableName.valueOf(GraphbaseConstants.NAMESPACE, graphId + TABLE_NAME_SUFFIX);
    }

    public static HTableDescriptor getHTableDescriptor(String graphId, boolean compression) {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(getTableName(graphId));

        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(FAMILY);
        hColumnDescriptor.setMaxVersions(Integer.MAX_VALUE);
        hColumnDescriptor.setKeepDeletedCells(KeepDeletedCells.TRUE);
        if (compression) {
            hColumnDescriptor.setCompressionType(Compression.Algorithm.LZ4);
        }
        hColumnDescriptor.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        hTableDescriptor.addFamily(hColumnDescriptor);
        return hTableDescriptor;
    }
}
