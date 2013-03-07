package org.lilyproject.indexer.model.indexerconf;

import java.io.IOException;
import java.util.List;

import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.RepositoryManager;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.util.repo.SystemFields;

public interface IndexUpdateBuilder {

    public void addField(String fieldName, List<String> value) throws InterruptedException, RepositoryException;

    public RepositoryManager getRepositoryManager();

    public SystemFields getSystemFields();

    public RecordContext getRecordContext();

    public void push(Record record, Dep dep);

    public void push(Record record, Record contextRecord, Dep dep);

    public RecordContext pop();

    public void addDependency(SchemaId field);

    public List<String> eval(Value value) throws RepositoryException, IOException, InterruptedException;

    public NameTemplateResolver getFieldNameResolver();

    public SchemaId getVTag();

    public String evalIndexFieldName(NameTemplate nameTemplate);

    /**
     * Returns the name of the repository table where the indexed record resides.
     */
    public String getTable();

}
