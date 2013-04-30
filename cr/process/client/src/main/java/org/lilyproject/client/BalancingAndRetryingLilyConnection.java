/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.client;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.lilyproject.indexer.Indexer;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.ConcurrentRecordUpdateException;
import org.lilyproject.repository.api.IOBlobException;
import org.lilyproject.repository.api.IORecordException;
import org.lilyproject.repository.api.IOTypeException;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.LRepository;
import org.lilyproject.repository.api.LTable;
import org.lilyproject.repository.api.RecordFactory;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.RetriesExhaustedBlobException;
import org.lilyproject.repository.api.RetriesExhaustedRecordException;
import org.lilyproject.repository.api.RetriesExhaustedTypeException;
import org.lilyproject.repository.api.TypeManager;
import org.lilyproject.repository.impl.AbstractRepositoryManager;
import org.lilyproject.repository.impl.RecordFactoryImpl;
import org.lilyproject.repository.impl.TenantTableKey;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;
import org.lilyproject.tenant.model.api.TenantModel;

/**
 * Creates a proxy around Repository, Indexer and TypeManager that automatically balances requests
 * over different Lily nodes, and can optionally retry operations when they fail due to
 * IO related exceptions or when no Lily servers are available.
 */
public class BalancingAndRetryingLilyConnection implements RepositoryManager {

    private final RepositoryManager repositoryManager;

    private final Indexer indexer;

    private final TenantModel tenantModel;

    private BalancingAndRetryingLilyConnection(RepositoryManager repositoryManager, Indexer indexer,
            TenantModel tenantModel) {
        this.repositoryManager = repositoryManager;
        this.indexer = indexer;
        this.tenantModel = tenantModel;
    }

    @Override
    public IdGenerator getIdGenerator() {
        return repositoryManager.getIdGenerator();
    }

    @Override
    public void close() throws IOException {
        // Do nothing
    }

    @Override
    public TypeManager getTypeManager() {
        return repositoryManager.getTypeManager();
    }

    @Override
    public RecordFactory getRecordFactory() {
        return repositoryManager.getRecordFactory();
    }

    @Override
    public LRepository getPublicRepository() throws InterruptedException, RepositoryException {
        return (Repository)repositoryManager.getPublicRepository();
    }

    @Override
    public LRepository getRepository(String tenantName) {
        try {
            return (Repository)repositoryManager.getRepository(tenantName);
        } catch (InterruptedException e) {
            // Same comment here as with the IOException -- it shouldn't be possible for this exception to be thrown in reality
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public LTable getTable(String tableName) throws InterruptedException, RepositoryException {
        return repositoryManager.getTable(tableName);
    }

    @Override
    public LTable getDefaultTable() throws InterruptedException, RepositoryException {
        return repositoryManager.getDefaultTable();
    }

    public Indexer getIndexer() {
        return indexer;
    }

    public static BalancingAndRetryingLilyConnection getInstance(final LilyClient lilyClient, TenantModel tenantModel) {

        InvocationHandler typeManagerHandler = new TypeManagerInvocationHandler(lilyClient);
        final TypeManager typeManager = (TypeManager) Proxy.newProxyInstance(TypeManager.class.getClassLoader(),
                new Class[]{TypeManager.class}, typeManagerHandler);



        InvocationHandler indexerHandler = new IndexerInvocationHandler(lilyClient);
        Indexer indexer = (Indexer) Proxy.newProxyInstance(Indexer.class.getClassLoader(),
                new Class[]{Indexer.class}, indexerHandler);

        final IdGenerator idGenerator = new IdGeneratorImpl();
        RecordFactory recordFactory = new RecordFactoryImpl(typeManager, idGenerator);

        RepositoryManager repositoryManager = new AbstractRepositoryManager(typeManager, idGenerator, recordFactory, tenantModel) {
            @Override
            protected Repository createRepository(TenantTableKey key) throws InterruptedException {
                InvocationHandler repositoryHandler = new RepositoryInvocationHandler(lilyClient, key, typeManager,
                        idGenerator);
                return (Repository) Proxy.newProxyInstance(Repository.class.getClassLoader(),
                        new Class[]{Repository.class}, repositoryHandler);
            }
        };

        return new BalancingAndRetryingLilyConnection(repositoryManager, indexer, tenantModel);
    }

    private static final class TypeManagerInvocationHandler extends RetryBase implements InvocationHandler {
        private final LilyClient lilyClient;

        TypeManagerInvocationHandler(LilyClient lilyClient) {
            super(lilyClient.getRetryConf());
            this.lilyClient = lilyClient;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equals("close")) {
                return null;
            }

            long startedAt = System.currentTimeMillis();
            int attempt = 0;

            while (true) {
                try {
                    TypeManager typeManager = lilyClient.getPlainRepository().getTypeManager();
                    return method.invoke(typeManager, args);
                } catch (NoServersException e) {
                    // Needs to be wrapped because NoServersException is not in the throws clause of the
                    // Repository & TypeManager methods
                    handleThrowable(new IOTypeException(e), method, startedAt, attempt, OperationType.TYPE);
                } catch (InterruptedException e) {
                    throw e;
                } catch (Throwable throwable) {
                    handleThrowable(throwable, method, startedAt, attempt, OperationType.TYPE);
                }
                attempt++;
            }
        }
    }

    private static final class RepositoryInvocationHandler extends RetryBase implements InvocationHandler {
        private final LilyClient lilyClient;
        private final TenantTableKey tenantTableKey;
        private final TypeManager typeManager;
        private final IdGenerator idGenerator;

        private RepositoryInvocationHandler(LilyClient lilyClient, TenantTableKey tenantTableKey,
                TypeManager typeManager, IdGenerator idGenerator) {
            super(lilyClient.getRetryConf());
            this.lilyClient = lilyClient;
            this.tenantTableKey = tenantTableKey;
            this.typeManager = typeManager;
            this.idGenerator = idGenerator;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equals("getTypeManager")) {
                return typeManager;
            } else if (method.getName().equals("getIdGenerator")) {
                // this method is here so that we would not go into the retry loop, which would throw an
                // RetriesExhausted checked exception, which would be a bit strange to declare for the
                // getIdGenerator method
                return idGenerator;
            } else if (method.getName().equals("close")) {
                return null;
            }

            long startedAt = System.currentTimeMillis();
            int attempt = 0;

            while (true) {
                try {
                    Repository repository = (Repository)lilyClient.getPlainTable(tenantTableKey.getTenantName(),
                            tenantTableKey.getTableName());
                    return method.invoke(repository, args);
                } catch (NoServersException e) {
                    // Needs to be wrapped because NoServersException is not in the throws clause of the
                    // Repository & TypeManager methods
                    if (isBlobMethod(method)) {
                        handleThrowable(new IOBlobException(e), method, startedAt, attempt, OperationType.BLOB);
                    } else {
                        handleThrowable(new IORecordException(e), method, startedAt, attempt, OperationType.RECORD);
                    }
                } catch (InterruptedException e) {
                    throw e;
                } catch (Throwable throwable) {
                    if (isBlobMethod(method)) {
                        handleThrowable(throwable, method, startedAt, attempt, OperationType.BLOB);
                    } else {
                        handleThrowable(throwable, method, startedAt, attempt, OperationType.RECORD);
                    }
                }

                attempt++;
            }
        }

        private boolean isBlobMethod(Method method) {
            if (method.getName().equals("delete")) {
                Class[] params = method.getParameterTypes();
                return params.length == 1 && Blob.class.isAssignableFrom(params[0].getClass());
            }
            return false;
        }
    }

    private static final class IndexerInvocationHandler extends RetryBase implements InvocationHandler {
        private final LilyClient lilyClient;

        private IndexerInvocationHandler(LilyClient lilyClient) {
            super(lilyClient.getRetryConf());
            this.lilyClient = lilyClient;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equals("close")) {
                return null;
            }

            long startedAt = System.currentTimeMillis();
            int attempt = 0;

            while (true) {
                try {
                    Indexer indexer = lilyClient.getPlainIndexer();
                    return method.invoke(indexer, args);
                } catch (NoServersException e) {
                    // Needs to be wrapped because NoServersException is not in the throws clause of the
                    // Repository & TypeManager methods
                    handleThrowable(new IORecordException(e), method, startedAt, attempt, OperationType.RECORD);
                } catch (InterruptedException e) {
                    throw e;
                } catch (Throwable throwable) {
                    handleThrowable(throwable, method, startedAt, attempt, OperationType.RECORD);
                }

                attempt++;
            }
        }
    }

    private enum OperationType {RECORD, TYPE, BLOB}

    private static class RetryBase {
        private Log log = LogFactory.getLog(getClass());
        private RetryConf retryConf;

        protected RetryBase(RetryConf retryConf) {
            this.retryConf = retryConf;
        }

        protected void handleThrowable(Throwable throwable, Method method, long startedAt, int attempt,
                                       OperationType opType) throws Throwable {

            if (throwable instanceof InvocationTargetException) {
                throwable = ((InvocationTargetException)throwable).getTargetException();
            }

            if (throwable instanceof IORecordException || throwable instanceof IOBlobException ||
                    throwable instanceof IOTypeException || throwable instanceof ConcurrentRecordUpdateException) {

                boolean callInitiated = true;
                if (throwable.getCause() instanceof NoServersException) {
                    // I initially thought we could also assume the request was not yet launched in case of
                    // ConnectException with msg "Connection refused". However, at least with the Avro HttpTransceiver,
                    // this exception can also occur when the connection is lost between writing the request
                    // and reading the response. On reading the response, the Java URLConnection will see
                    // there is no connection anymore and reestablish it, hence giving a "connection refused" error.
                    // In this situation, the request is sent out by the server, so it is not safe to simply redo it.
                    callInitiated = false;
                }
                handleRetry(method, startedAt, attempt, callInitiated, throwable, opType);
            } else {
                throw throwable;
            }
        }

        protected void handleRetry(Method method, long startedAt, int attempt,
                                   boolean callInitiated, Throwable throwable, OperationType opType) throws Throwable {

            long timeSpentRetrying = System.currentTimeMillis() - startedAt;
            if (timeSpentRetrying > retryConf.getRetryMaxTime()) {
                switch (opType) {
                    case RECORD:
                        throw new RetriesExhaustedRecordException(getOpString(method), attempt, timeSpentRetrying,
                                throwable);
                    case TYPE:
                        throw new RetriesExhaustedTypeException(getOpString(method), attempt, timeSpentRetrying,
                                throwable);
                    case BLOB:
                        throw new RetriesExhaustedBlobException(getOpString(method), attempt, timeSpentRetrying,
                                throwable);
                    default:
                        throw new RuntimeException("This should never occur: unhandled op type: " + opType);
                }
            }

            String methodName = method.getName();

            boolean retry = false;

            // Since the "newSomething" methods are simple factory methods, put them in the same class as reads
            // TODO: the methods starting with get include the blob methods getInputStream and getOutputStream,
            //       which should probably have a different treatment
            if ((methodName.startsWith("read") || methodName.startsWith("get") || methodName.startsWith("new"))
                    && retryConf.getRetryReads()) {
                retry = true;
            } else if (methodName.equals("createOrUpdate") && retryConf.getRetryCreateOrUpdate()) {
                retry = true;
            } else if (methodName.startsWith("update") && retryConf.getRetryUpdates()) {
                retry = true;
            } else if (methodName.startsWith("delete") && retryConf.getRetryDeletes()) {
                retry = true;
            } else if (methodName.startsWith("create") && retryConf.getRetryCreate() &&
                    (!callInitiated || retryConf.getRetryCreateRiskDoubles())) {
                retry = true;
            }

            if (retry) {
                int sleepTime = getSleepTime(attempt);
                if (log.isDebugEnabled() || log.isInfoEnabled()) {
                    String message = "Sleeping " + sleepTime + "ms before retrying operation " +
                            getOpString(method) + " attempt " + attempt +
                            " failed due to " + throwable.getCause().toString();
                    if (log.isDebugEnabled()) {
                        log.debug(message, throwable);
                    } else if (log.isInfoEnabled()) {
                        log.info(message);
                    }
                }
                Thread.sleep(sleepTime);
            } else {
                throw throwable;
            }
        }

        private int getSleepTime(int attempt) throws InterruptedException {
            int pos =
                    attempt < retryConf.getRetryIntervals().length ? attempt : retryConf.getRetryIntervals().length - 1;
            return retryConf.getRetryIntervals()[pos];
        }

        private String getOpString(Method method) {
            return method.getDeclaringClass().getSimpleName() + "." + method.getName();
        }
    }
}
