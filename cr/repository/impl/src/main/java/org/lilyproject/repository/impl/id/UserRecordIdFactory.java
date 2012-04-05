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
package org.lilyproject.repository.impl.id;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.repository.api.RecordId;

public class UserRecordIdFactory implements RecordIdFactory {
    protected static final byte VARIANT_SEPARATOR = (byte)0;
    
    @Override
    public DataInput[] splitInMasterAndVariant(DataInput dataInput) {
        // Search for separator byte
        int sepPos = dataInput.indexOf(VARIANT_SEPARATOR);
        
        if (sepPos == -1) {
            return new DataInput[] { dataInput, null };
        } else {
            DataInput keyInput = new DataInputImpl(((DataInputImpl)dataInput), dataInput.getPosition(), sepPos);

            DataInput variantInput = new DataInputImpl(((DataInputImpl)dataInput), sepPos + 1, dataInput.getSize());

            return new DataInput[] { keyInput, variantInput };
        }
    }

    @Override
    public RecordId fromBytes(DataInput dataInput, IdGeneratorImpl idGenerator) {
        if (dataInput.indexOf((byte)0) != -1) {
            throw new IllegalArgumentException("The NULL character is not allowed in USER record id's.");
        }

        String id = dataInput.readUTF(dataInput.getSize() - dataInput.getPosition());
        return new UserRecordId(id, idGenerator);
    }

    @Override
    public RecordId fromString(String string, IdGeneratorImpl idGenerator) {
        if (string.indexOf(0) != -1) {
            throw new IllegalArgumentException("The NULL character is not allowed in USER record id's.");
        }
        return new UserRecordId(string, idGenerator);
    }
}
