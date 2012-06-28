package org.lilyproject.indexer.model.indexerconf;

import java.util.List;

import com.google.common.base.Predicate;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.util.repo.VTaggedRecord;

public class IndexField implements MappingNode {

    private final String name;
    private final Value value;

    public IndexField(String name, Value value) {
        this.name = name;
        this.value = value;
    }

    public Value getValue() {
        return value;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean isIndexAffectedByUpdate(VTaggedRecord vtRecord, Scope scope) throws InterruptedException,
            RepositoryException {
        return vtRecord.getUpdatedFieldTypeIdsByScope().get(scope).contains(value.getFieldDependency());
    }

    @Override
    public void visitAll(Predicate<MappingNode> predicate) {
        predicate.apply(this);
    }

    @Override
    public void collectIndexFields(List<IndexField> indexFields, Record record, long version, SchemaId vtag) {
        indexFields.add(this);
    }

}
