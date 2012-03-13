package org.lilyproject.mapreduce;

import org.apache.hadoop.io.Writable;
import org.lilyproject.repository.api.Record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecordWritable implements Writable {    
    private Record record;

    protected RecordWritable() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Record getRecord() {
        return record;
    }

    public void setRecord(Record record) {
        this.record = record;
    }
}
