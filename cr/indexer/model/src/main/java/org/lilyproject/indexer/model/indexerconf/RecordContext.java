package org.lilyproject.indexer.model.indexerconf;

import java.util.Stack;

import org.lilyproject.repository.api.Record;

public class RecordContext {

    // FIXME: replace with Stack<FollowRecord>
    private Stack<Record> contextRecords = new Stack<Record>();
    private Stack<Record> records = new Stack<Record>();

    public RecordContext(Record root) {
        push(root);
    }

    public void push(Record record) {
        contextRecords.push(record);
        records.push(record);
    }

    public void pushEmbedded(Record record) {
        contextRecords.push(last());
        records.push(record);
    }

    public void pop() {
        contextRecords.pop();
        records.pop();
    }

    public Record lastReal() {
        return contextRecords.peek();
    }

    public Record last() {
        return records.peek();
    }

    public FollowRecord newFollow() {
        return new FollowRecord(last(), lastReal());
    }

}
