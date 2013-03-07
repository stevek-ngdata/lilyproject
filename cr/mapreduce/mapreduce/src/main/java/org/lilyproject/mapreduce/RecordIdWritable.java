/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.mapreduce;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.impl.id.IdGeneratorImpl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A Hadoop WritableComparable for Lily RecordId's.
 *
 * <p>Stores the length of the record id as a vint, followed by the normal binary
 * representation of the record id (= the same as used as row key in the HBase table).</p>
 */
public class RecordIdWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {
    private RecordId recordId;
    private static final IdGenerator ID_GENERATOR = new IdGeneratorImpl();

    public RecordIdWritable() {
    }

    public RecordIdWritable(RecordId recordId) {
        this.recordId = recordId;
    }

    @Override
    public int getLength() {
        // RecordId's cache their binary representation, so this should be fast
        return recordId.toBytes().length;
    }

    @Override
    public byte[] getBytes() {
        return recordId.toBytes();
    }

    // Disclaimer: the Comparator was copied from Hadoop's Text class.
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(RecordIdWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1,
                byte[] b2, int s2, int l2) {
            // don't include the vint length in the comparison
            int n1 = WritableUtils.decodeVIntSize(b1[s1]);
            int n2 = WritableUtils.decodeVIntSize(b2[s2]);
            return compareBytes(b1, s1+n1, l1-n1, b2, s2+n2, l2-n2);
        }
    }

    static {
        // register this comparator
        WritableComparator.define(RecordIdWritable.class, new Comparator());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        byte[] bytes = recordId.toBytes();
        WritableUtils.writeVInt(out, bytes.length);
        out.write(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = WritableUtils.readVInt(in);
        byte[] bytes = new byte[length];
        in.readFully(bytes, 0, length);
        this.recordId = ID_GENERATOR.fromBytes(bytes);
    }

    public RecordId getRecordId() {
        return recordId;
    }

    public void setRecordId(RecordId recordId) {
        this.recordId = recordId;
    }
}
