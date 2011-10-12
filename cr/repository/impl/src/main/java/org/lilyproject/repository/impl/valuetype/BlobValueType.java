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
package org.lilyproject.repository.impl.valuetype;

import java.util.Comparator;
import java.util.IdentityHashMap;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.hbaseext.ContainsValueComparator;
import org.lilyproject.repository.api.*;

public class BlobValueType extends AbstractValueType implements ValueType {
    public final static String NAME = "BLOB";

    @Override
    public String getBaseName() {
        return NAME;
    }
    
    @Override
    public ValueType getDeepestValueType() {
        return this;
    }

    /**
     * See write for the byte format.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Blob read(DataInput dataInput) {
        // Read the encoding version byte, but ignore it for the moment since there is only one encoding
        dataInput.readByte();
        int keyLength = dataInput.readVInt();
        byte[] key = null;
        if (keyLength > 0) {
            key = dataInput.readBytes(keyLength);
        }
        String mediaType = dataInput.readUTF();
        Long size = dataInput.readLong();
        if (size == -1) {
            size = null;
        }
        String filename = dataInput.readUTF();
        return new Blob(key, mediaType, size, filename);
    }

    /**
     * Format of the bytes written :
     * - Length of the blob value : int of 4 bytes
     * - Blob Value
     * - Blob Media Type : UTF (which starts with an int of 4 bytes indicating its length)
     * - Blob size : long of 8 bytes
     * - Blob name : UTF (which starts with an int of 4 bytes indicating its length)
     * 
     * <p> IMPORTANT: Any changes on this format has an impact on the {@link ContainsValueComparator}
     */
    @Override
    public void write(Object value, DataOutput dataOutput, IdentityHashMap<Record, Object> parentRecords) {
        dataOutput.writeByte((byte)1); // Encoding version 1
        Blob blob = (Blob)value;
        byte[] key = blob.getValue();
        if (key == null) {
            dataOutput.writeVInt(0);
        } else {
            dataOutput.writeVInt(key.length);
            dataOutput.writeBytes(key);
        }
        dataOutput.writeUTF(blob.getMediaType());
        Long size = blob.getSize();
        if (size == null) {
            size = Long.valueOf(-1);
        }
        dataOutput.writeLong(size);
        dataOutput.writeUTF(blob.getName());
    }

    @Override
    public Class getType() {
        return Blob.class;
    }

    @Override
    public Comparator getComparator() {
        return null;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + NAME.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        return true;
    }

    //
    // Factory
    //
    public static ValueTypeFactory factory() {
        return new BlobValueTypeFactory();
    }
    
    public static class BlobValueTypeFactory implements ValueTypeFactory {
        private static BlobValueType instance = new BlobValueType();
        
        @Override
        public ValueType getValueType(String typeParams) {
            return instance;
        }
    }
}
