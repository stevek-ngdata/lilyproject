package org.lilyproject.repotestfw;

import java.io.IOException;
import java.util.List;

import org.lilyproject.repository.api.RepositoryTable;
import org.lilyproject.repository.api.TableCreateDescriptor;
import org.lilyproject.repository.api.TableManager;

public class FakeTableManager implements TableManager {
    @Override
    public RepositoryTable createTable(String s) throws InterruptedException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RepositoryTable createTable(TableCreateDescriptor tableCreateDescriptor) throws InterruptedException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(String s) throws InterruptedException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<RepositoryTable> getTables() throws InterruptedException, IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tableExists(String s) throws InterruptedException, IOException {
        throw new UnsupportedOperationException();
    }
}
