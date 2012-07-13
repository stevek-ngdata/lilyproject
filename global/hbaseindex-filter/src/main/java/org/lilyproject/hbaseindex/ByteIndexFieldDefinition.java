/*
 * Copyright 2010 Outerthought bvba
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
package org.lilyproject.hbaseindex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gotometrics.orderly.FixedByteArrayRowKey;
import com.gotometrics.orderly.RowKey;
import com.gotometrics.orderly.Termination;
import org.codehaus.jackson.node.ObjectNode;

/**
 * This kind of field allows to store arbitrary bytes (a byte array) in the index key, the ideal fallback the case none
 * of the other types suite your needs. Only for fixed-length byte arrays, if the provided value is longer it will be
 * cut off, otherwise it will be padded with zeros.
 */
public class ByteIndexFieldDefinition extends IndexFieldDefinition {
    private int length;

    public ByteIndexFieldDefinition() {
        // hadoop serialization
    }

    public ByteIndexFieldDefinition(String name, int length) {
        super(name);

        this.length = length;
    }

    public ByteIndexFieldDefinition(String name, ObjectNode jsonObject) {
        this(name, jsonObject.get("length") != null ? jsonObject.get("length").getIntValue() : -1);
    }

    public int getLength() {
        return length;
    }

    @Override
    RowKey asRowKey() {
        final FixedByteArrayRowKey rowKey = new FixedByteArrayRowKey(length);
        rowKey.setOrder(this.getOrder());
        return rowKey;
    }

    @Override
    RowKey asRowKeyWithoutTermination() {
        final RowKey rowKey = asRowKey();
        rowKey.setTermination(Termination.SHOULD_NOT);
        return rowKey;
    }

    @Override
    public ObjectNode toJson() {
        ObjectNode object = super.toJson();
        object.put("length", length);
        return object;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(length);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        length = in.readInt();
    }
}
