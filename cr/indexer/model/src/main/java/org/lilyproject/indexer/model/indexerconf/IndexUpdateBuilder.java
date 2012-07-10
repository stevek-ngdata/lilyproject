package org.lilyproject.indexer.model.indexerconf;

import java.util.List;

import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;

public interface IndexUpdateBuilder {

    public void addField(String fieldName, List<String> value) throws InterruptedException, RepositoryException;

    public Repository getRepository();

    public RecordContext getRecordContext();
    public void push(Record record, Dep dep);
    public void push(Record record, Record contextRecord, Dep dep);
    public RecordContext pop();
    public void addDependency(SchemaId field);

    public List<String> eval(Value value) throws RepositoryException, InterruptedException;

    public NameTemplateResolver getFieldNameResolver();

    public SchemaId getVTag();

    public String evalIndexFieldName(NameTemplate nameTemplate);

}
