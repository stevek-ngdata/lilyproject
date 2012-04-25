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

import org.codehaus.jackson.node.ObjectNode;

/**
 * This kind of field allows to store arbitrary bytes (a byte array)
 * in the index key, the ideal fallback the case none of the other
 * types suite your needs. Only for fixed-length byte arrays, if the
 * provided value is longer it will be cut off, otherwise it will
 * be padded with zeros.
 */
public class ByteIndexFieldDefinition extends IndexFieldDefinition {
    private int length;

    public ByteIndexFieldDefinition(String name, int length) {
        super(name, IndexValueType.BYTES);

        this.length = length;
    }

    public ByteIndexFieldDefinition(String name, ObjectNode jsonObject) {
        super(name, IndexValueType.BYTES, jsonObject);

        if (jsonObject.get("length") != null)
            this.length = jsonObject.get("length").getIntValue();
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public byte[] toBytes(Object value) {
        byte[] byteValue = (byte[])value;
        if (byteValue.length == getLength())
            return byteValue;

        byte[] bytes = new byte[getLength()];

        int copyLength = byteValue.length < length ? byteValue.length : length;
        System.arraycopy(byteValue, 0, bytes, 0, copyLength);

        return bytes;
    }

    @Override
    public ObjectNode toJson() {
        ObjectNode object = super.toJson();
        object.put("length", length);
        return object;
    }

}
