package org.lilyproject.indexer.model.indexerconf;

import java.util.List;

import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;

public interface IndexUpdateBuilder {

    public void addField(String fieldName, List<String> value) throws InterruptedException, RepositoryException;

    public Repository getRepository();

    public RecordContext getRecordContext();

    public List<String> eval(Value value) throws RepositoryException, InterruptedException;

    public List<FollowRecord> evalFollow(Follow follow) throws RepositoryException, InterruptedException;

    public NameTemplateResolver getNameResolver();

}
