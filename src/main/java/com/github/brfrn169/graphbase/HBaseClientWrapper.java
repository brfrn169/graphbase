package com.github.brfrn169.graphbase;

import com.github.brfrn169.graphbase.exception.GraphbaseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

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
            throw new GraphbaseException("an error occurred during creating hbase namespace", e);
        }
    }

    public void createTable(HTableDescriptor hTableDescriptor) {
        try (Admin admin = connection.getAdmin()) {
            try {
                if (!admin.tableExists(hTableDescriptor.getTableName()))
                    admin.createTable(hTableDescriptor);
            } catch (TableExistsException e) {
                LOG.warn("table already exists. tableName=" + hTableDescriptor.getTableName(), e);
            }
        } catch (IOException e) {
            throw new GraphbaseException("an error occurred during creating hbase table", e);
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
            throw new GraphbaseException("an error occurred during creating hbase table", e);
        }
    }

    public void dropTable(TableName tableName) {
        try (Admin admin = connection.getAdmin()) {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        } catch (TableNotFoundException e) {
            LOG.warn("table not found. tableName=" + tableName, e);
        } catch (IOException e) {
            throw new GraphbaseException("an error occurred during deleting hbase table", e);
        }
    }
}
