package org.lilyproject.indexer.model.indexerconf;

import java.io.IOException;
import java.util.List;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RepositoryException;

public class RecordFieldFollow implements Follow {
    private FieldType fieldType;

    public RecordFieldFollow(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    @Override
    public void follow(IndexUpdateBuilder indexUpdateBuilder, FollowCallback followCallback) throws RepositoryException, IOException, InterruptedException {
        if (!indexUpdateBuilder.getSystemFields().isSystemField(fieldType.getName())) {
            indexUpdateBuilder.addDependency(fieldType.getId());
        }
        RecordContext ctx = indexUpdateBuilder.getRecordContext();
        if (ctx.record != null) {
            List records = IndexerUtils.flatList(ctx.record, fieldType);
            for (Record record: (List<Record>)records) {
                indexUpdateBuilder.push(record, ctx.contextRecord, ctx.dep); // TODO: pass null instead of ctx.dep?
                followCallback.call();
                indexUpdateBuilder.pop();
            }
        }
    }
}
