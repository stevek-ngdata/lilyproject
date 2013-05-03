package org.lilyproject.repository.remote;

import org.apache.avro.ipc.Transceiver;
import org.lilyproject.avro.AvroConverter;
import org.lilyproject.avro.AvroGenericException;
import org.lilyproject.avro.AvroIOException;
import org.lilyproject.avro.AvroLily;
import org.lilyproject.avro.AvroTableCreateDescriptor;
import org.lilyproject.repository.api.RepositoryTable;
import org.lilyproject.repository.api.TableCreateDescriptor;
import org.lilyproject.repository.api.TableManager;
import org.lilyproject.repository.impl.RepositoryTableImpl;
import org.lilyproject.util.io.Closer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Remote (avro) implementation of TableManager.
 *
 * <p>Remotely we can also talk directly to HBase, but the reason to go through the lily server is to
 * make sure table creation settings are applied.</p>
 */
public class RemoteTableManager implements TableManager {
    private String tenantName;
    private AvroLily lilyProxy;
    private AvroConverter converter;
    private Transceiver client;

    public RemoteTableManager(String tenantName, AvroLilyTransceiver lilyTransceiver, AvroConverter converter) throws IOException {
        this.tenantName = tenantName;
        this.converter = converter;
        client = lilyTransceiver.getTransceiver();
        lilyProxy = lilyTransceiver.getLilyProxy();
    }

    public void close() throws IOException {
        // TODO multitenancy study the lifecycle
        Closer.close(client);
    }

    @Override
    public RepositoryTable createTable(String tableName) throws InterruptedException, IOException {
        try {
            AvroTableCreateDescriptor descriptor = new AvroTableCreateDescriptor();
            descriptor.setName(tableName);
            lilyProxy.createTable(tenantName, descriptor);
            return new RepositoryTableImpl(tableName);
        } catch (AvroIOException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public RepositoryTable createTable(TableCreateDescriptor descriptor) throws InterruptedException, IOException {
        try {
            lilyProxy.createTable(tenantName, converter.convert(descriptor));
            return new RepositoryTableImpl(descriptor.getName());
        } catch (AvroIOException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public void dropTable(String tableName) throws InterruptedException, IOException {
        try {
            lilyProxy.dropTable(tenantName, tableName);
        } catch (AvroIOException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public List<RepositoryTable> getTables() throws InterruptedException, IOException {
        try {
            List<String> tableNames = lilyProxy.getTables(tenantName);
            List<RepositoryTable> tables = new ArrayList<RepositoryTable>(tableNames.size());
            for (String name : tableNames) {
                tables.add(new RepositoryTableImpl(name));
            }
            return tables;
        } catch (AvroIOException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        }
    }

    @Override
    public boolean tableExists(String tableName) throws InterruptedException, IOException {
        try {
            return lilyProxy.tableExists(tenantName, tableName);
        } catch (AvroIOException e) {
            throw converter.convert(e);
        } catch (AvroGenericException e) {
            throw converter.convert(e);
        }
    }
}
