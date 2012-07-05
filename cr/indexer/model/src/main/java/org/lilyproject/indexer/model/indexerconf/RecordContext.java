package org.lilyproject.indexer.model.indexerconf;

import java.util.Stack;

import org.lilyproject.repository.api.Record;

public class RecordContext {

    private Stack<FollowRecord> followRecords = new Stack<FollowRecord>();

    public RecordContext(Record root) {
        push(new FollowRecord(root,root));
    }

    public void push(FollowRecord record) {
        followRecords.push(record);
    }

    public void pop() {
        followRecords.pop();
    }

    public FollowRecord last() {
        return followRecords.peek();
    }

}
