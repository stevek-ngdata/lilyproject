package org.lilyproject.indexer.model.indexerconf;

import java.util.List;
import java.util.Set;

import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;

public interface IndexUpdateBuilder {

    public void addField(String fieldName, List<String> value) throws InterruptedException, RepositoryException;

    public Repository getRepository();

    public RecordContext getRecordContext();

    public List<String> eval(Value value) throws RepositoryException, InterruptedException;

    public List<FollowRecord> evalFollow(Follow follow) throws RepositoryException, InterruptedException;

    public NameTemplateResolver getNameResolver();

    public void addDependency(RecordId id, Set<String> moreDimensions, SchemaId field);

}
