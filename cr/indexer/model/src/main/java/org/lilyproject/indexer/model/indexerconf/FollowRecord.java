package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.Record;

/**
 * Combines a record object together with the record that needs to be used for evaluating links.
 *
 * <p>For real (non-nested) records, the two record objects are the same. In case of nested records,
 * the contextRecord is the real record to which it belongs. Nested records don't have ID's, thus
 * can't be used for resolving links.
 */
public class FollowRecord {
    public Record record;
    public Record contextRecord;

    public FollowRecord(Record record, Record contextRecord) {
        this.record = record;
        this.contextRecord = contextRecord;
    }
}

