package org.lilyproject.indexer.model.indexerconf;

import java.io.IOException;

import org.lilyproject.repository.api.RepositoryException;

public interface FollowCallback {
    public void call() throws RepositoryException, IOException, InterruptedException;
}
