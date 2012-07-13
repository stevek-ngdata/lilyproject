package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.RepositoryException;

public interface FollowCallback {
    public void call() throws RepositoryException, InterruptedException;
}
