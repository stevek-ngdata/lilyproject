package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.Record;

public class RecordContext {

    public final Record contextRecord;
    public final Record record;
    public final Dep dep;

    public RecordContext(Record record, Dep dep) {
        this.contextRecord = record;
        this.record = record;
        this.dep = dep;
    }

    public RecordContext(Record record, Record contextRecord, Dep dep) {
        this.record = record;
        this.contextRecord = contextRecord;
        this.dep = dep;
    }

}
