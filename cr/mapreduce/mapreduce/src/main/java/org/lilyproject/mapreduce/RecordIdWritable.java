package org.lilyproject.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.lilyproject.repository.api.RecordId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecordIdWritable implements WritableComparable<RecordIdWritable> {
    private RecordId recordId;

    @Override
    public int compareTo(RecordIdWritable o) {
        // TODO
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // TODO
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // TODO
    }

    public RecordId getRecordId() {
        return recordId;
    }

    public void setRecordId(RecordId recordId) {
        this.recordId = recordId;
    }
}
