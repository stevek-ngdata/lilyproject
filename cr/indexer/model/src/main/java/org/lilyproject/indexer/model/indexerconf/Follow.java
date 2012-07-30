package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.RepositoryException;

public interface Follow {

    void follow(IndexUpdateBuilder indexUpdateBuilder, FollowCallback callback) throws RepositoryException, InterruptedException;
}
