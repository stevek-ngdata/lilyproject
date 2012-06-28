package org.lilyproject.indexer.model.indexerconf;

import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.repository.api.SchemaId;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.util.repo.VTaggedRecord;

public abstract class ContainerMappingNode implements MappingNode {

    private final List<MappingNode> children = Lists.newArrayList();

    public void addChildNode(MappingNode node) {
        children.add(node);
    }

    public List<MappingNode> getChildren() {
        return children;
    }

    public boolean childrenAffectedByUpdate(VTaggedRecord record, Scope scope) throws InterruptedException, RepositoryException {
        for (MappingNode child: getChildren()) {
            if (child.isIndexAffectedByUpdate(record, scope)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void collectIndexFields(List<IndexField> indexFields, Record record, long version, SchemaId vtag) {
        for (MappingNode child: getChildren()) {
            child.collectIndexFields(indexFields, record, version, vtag);
        }
    }

    public void visitAll(Predicate<MappingNode> predicate) {
        if (predicate.apply(this)) {
            for (MappingNode child: children) {
                child.visitAll(predicate);
            }
        }
    }



}