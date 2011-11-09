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
package org.lilyproject.repository.impl.primitivevaluetype;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.api.DataOutput;
import org.lilyproject.repository.api.IdGenerator;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.PrimitiveValueType;
import org.lilyproject.repository.api.RecordId;

/**
 *
 */
public class LinkValueType implements PrimitiveValueType {
    
    private final String NAME = "LINK";
    private final IdGenerator idGenerator;

    public LinkValueType(IdGenerator idGenerator) {
        this.idGenerator = idGenerator;
        
    }
    
    public String getName() {
        return NAME;
    }

    public Link read(DataInput dataInput) {
        // Read the encoding version byte, but ignore it for the moment since there is only one encoding
        dataInput.readByte();
        return Link.read(dataInput, idGenerator);
    }

    public void write(Object value, DataOutput dataOutput) {
        dataOutput.writeByte((byte)1); // Encoding version 1
        ((Link)value).write(dataOutput);
    }

    public Class getType() {
        return RecordId.class;
    }
}
