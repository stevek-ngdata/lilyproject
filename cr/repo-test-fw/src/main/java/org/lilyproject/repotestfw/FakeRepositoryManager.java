package org.lilyproject.repotestfw;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.TableManager;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.RecordFactoryImpl;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.util.hbase.RepoAndTableUtil;

public class FakeRepositoryManager implements RepositoryManager {
    private LoadingCache<String, LRepository> repositories;
    private Repository publicRepository;
    private TableManager tableManager;
    private TypeManager typeManager;
    private IdGenerator idGenerator;
    private RecordFactory recordFactory;
    private CacheLoader<String,LRepository> repositoryLoader;

    public static RepositoryManager bootstrapRepositoryManager() {
        TableManager tableManager = new FakeTableManager();
        IdGenerator idGenerator = new IdGeneratorImpl();
        TypeManager typeManager = new FakeTypeManager(idGenerator);
        return new FakeRepositoryManager(
                tableManager,
                typeManager,
                idGenerator,
                new RecordFactoryImpl()
        );
    }

    public FakeRepositoryManager(final TableManager tableManager, final TypeManager typeManager,
                                 final IdGenerator idGenerator, final RecordFactory recordFactory) {
        this.tableManager = tableManager;
        this.typeManager = typeManager;
        this.idGenerator = idGenerator;
        this.recordFactory = recordFactory;
        setRepositoryLoader(new DummyRepositoryLoader(this));
    }

    @Override
    public LRepository getDefaultRepository() throws InterruptedException, RepositoryException {
        try {
            return repositories.get(RepoAndTableUtil.DEFAULT_REPOSITORY);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public LRepository getRepository(String repositoryName) throws InterruptedException, RepositoryException {
        try {
            return repositories.get(repositoryName);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        // no closing needed
    }

    public void setRepositoryLoader(CacheLoader<String,LRepository> repositoryLoader) {
        this.repositoryLoader = repositoryLoader;
        repositories = CacheBuilder.newBuilder().build(this.repositoryLoader);
    }

    public static class DummyRepositoryLoader extends CacheLoader<String, LRepository> {
        private final FakeRepositoryManager fakeRepositoryManager;
        public DummyRepositoryLoader (FakeRepositoryManager manager) {
            this.fakeRepositoryManager = manager;
        }
        @Override
        public LRepository load(String repositoryName) throws Exception {
            return new FakeLRepository(fakeRepositoryManager,
                    repositoryName,
                    fakeRepositoryManager.tableManager,
                    fakeRepositoryManager.idGenerator,
                    fakeRepositoryManager.typeManager,
                    fakeRepositoryManager.recordFactory);
        }
    }
}
