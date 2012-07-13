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

import com.gotometrics.orderly.RowKey;
import com.gotometrics.orderly.Termination;
import com.gotometrics.orderly.VariableLengthByteArrayRowKey;
import org.codehaus.jackson.node.ObjectNode;

/**
 * This kind of field allows to store arbitrary byte arrays in the index key, the ideal fallback the case none of the
 * other types suite your needs. It differs from ByteIndexFieldDefinition in that it supports variable length bytes. It
 * does this by interpreting the byte array as an unsigned decimal number and then applying (customized) "binary coded
 * decimal" encoding (http://en.wikipedia.org/wiki/Binary-coded_decimal).
 */
public class VariableLengthByteIndexFieldDefinition extends IndexFieldDefinition {

    private int fixedPrefixLength;

    public VariableLengthByteIndexFieldDefinition() {
        // hadoop serialization
    }

    public VariableLengthByteIndexFieldDefinition(String name, ObjectNode jsonObject) {
        this(name, jsonObject.get("fixedPrefixLength") != null ? jsonObject.get("fixedPrefixLength").getIntValue() : 0);
    }

    public VariableLengthByteIndexFieldDefinition(String name, int fixedPrefixLength) {
        super(name);

        this.fixedPrefixLength = fixedPrefixLength;
    }

    public VariableLengthByteIndexFieldDefinition(String name) {
        this(name, 0);
    }

    @Override
    RowKey asRowKey() {
        final VariableLengthByteArrayRowKey rowKey = new VariableLengthByteArrayRowKey(this.fixedPrefixLength);
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
        object.put("fixedPrefixLength", fixedPrefixLength);
        return object;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(fixedPrefixLength);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        fixedPrefixLength = in.readInt();
    }
}
