package org.lilyproject.tools.import_.cli;

/**
 * Wrapper of ImportListener which makes all calls synchronized.
 */
public class SynchronizedImportListener implements ImportListener {
    private ImportListener delegate;

    public SynchronizedImportListener(ImportListener listener) {
        this.delegate = listener;
    }

    @Override
    public void exception(Throwable throwable) {
        delegate.exception(throwable);
    }

    @Override
    public synchronized void conflict(EntityType entityType, String entityName, String propName, Object oldValue,
            Object newValue) throws ImportConflictException {
        delegate.conflict(entityType, entityName, propName, oldValue, newValue);
    }

    @Override
    public synchronized void existsAndEqual(EntityType entityType, String entityName, String entityId) {
        delegate.existsAndEqual(entityType, entityName, entityId);
    }

    @Override
    public synchronized void updated(EntityType entityType, String entityName, String entityId, long version) {
        delegate.updated(entityType, entityName, entityId, version);
    }

    @Override
    public synchronized void created(EntityType entityType, String entityName, String entityId) {
        delegate.created(entityType, entityName, entityId);
    }
}
