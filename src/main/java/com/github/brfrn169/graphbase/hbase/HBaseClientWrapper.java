package com.github.brfrn169.graphbase.hbase;

import com.github.brfrn169.graphbase.exception.GraphbaseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;

import java.io.Closeable;
import java.io.IOException;

public class HBaseClientWrapper implements Closeable {

    private static final Log LOG = LogFactory.getLog(HBaseClientWrapper.class);

    private final Connection connection;

    public HBaseClientWrapper(Configuration conf) {
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new GraphbaseException("fail to create a hbase connection", e); // TODO
        }
    }

    @Override public void close() throws IOException {
        connection.close();
    }

    public static void addMutations(RowMutations mutations, Put put) {
        try {
            mutations.add(put);
        } catch (IOException e) {
            throw new GraphbaseException("an error occurred during addMutations", e);
        }
    }

    public static void addMutations(RowMutations mutations, Delete delete) {
        try {
            mutations.add(delete);
        } catch (IOException e) {
            throw new GraphbaseException("an error occurred during addMutations", e); // TODO
        }
    }

    public void createNamespace(String namespace) {
        try (Admin admin = connection.getAdmin()) {
            try {
                admin.getNamespaceDescriptor(namespace);
            } catch (NamespaceNotFoundException e) {
                // create namespace only when the namespace is not found
                try {
                    admin.createNamespace(NamespaceDescriptor.create(namespace).build());
                    LOG.info("created namespace. namespace=" + namespace);
                } catch (NamespaceExistException ex) {
                    LOG.debug("namespace already exists. namespace=" + namespace, ex);
                }
            }
        } catch (IOException e) {
            throw new GraphbaseException("an error occurred during createNamespace", e);
        }
    }

    public void createTable(HTableDescriptor hTableDescriptor, byte[][] splitKeys) {
        try (Admin admin = connection.getAdmin()) {
            try {
                if (!admin.tableExists(hTableDescriptor.getTableName()))
                    admin.createTable(hTableDescriptor, splitKeys);
            } catch (TableExistsException e) {
                LOG.warn("table already exists. tableName=" + hTableDescriptor.getTableName(), e);
            }
        } catch (IOException e) {
            throw new GraphbaseException("an error occurred during createTable", e);
        }
    }

    public void deleteTable(TableName tableName) {
        try (Admin admin = connection.getAdmin()) {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        } catch (TableNotFoundException e) {
            LOG.warn("table not found. tableName=" + tableName, e);
        } catch (IOException e) {
            throw new GraphbaseException("an error occurred during deleteTable", e);
        }
    }

    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put,
        TableName tableName) {
        try (Table table = connection.getTable(tableName)) {
            return table.checkAndPut(row, family, qualifier, value, put);
        } catch (IOException e) {
            throw new GraphbaseException("an error occurred during checkAndPut", e);
        }
    }

    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,
        Delete delete, TableName tableName) {
        try (Table table = connection.getTable(tableName)) {
            return table.checkAndDelete(row, family, qualifier, value, delete);
        } catch (IOException e) {
            throw new GraphbaseException("an error occurred during checkAndDelete", e);
        }
    }

    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
        CompareFilter.CompareOp compareOp, byte[] value, Delete delete, TableName tableName) {
        try (Table table = connection.getTable(tableName)) {
            return table.checkAndDelete(row, family, qualifier, compareOp, value, delete);
        } catch (IOException e) {
            throw new GraphbaseException("an error occurred during checkAndDelete", e);
        }
    }

    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
        CompareFilter.CompareOp compareOp, byte[] value, RowMutations mutation,
        TableName tableName) {
        try (Table table = connection.getTable(tableName)) {
            return table.checkAndMutate(row, family, qualifier, compareOp, value, mutation);
        } catch (IOException e) {
            throw new GraphbaseException("an error occurred during checkAndMutate", e);
        }
    }

    public void mutateRow(RowMutations mutation, TableName tableName) {
        try (Table table = connection.getTable(tableName)) {
            table.mutateRow(mutation);
        } catch (IOException e) {
            throw new GraphbaseException("an error occurred during mutateRow", e);
        }
    }

    public boolean exists(Get get, TableName tableName) {
        try (Table table = connection.getTable(tableName)) {
            return table.exists(get);
        } catch (IOException e) {
            throw new GraphbaseException("an error occurred during exists", e);
        }
    }

    public Result get(Get get, TableName tableName) {
        try (Table table = connection.getTable(tableName)) {
            return table.get(get);
        } catch (IOException e) {
            throw new GraphbaseException("an error occurred during get", e);
        }
    }
}
