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
import org.lilyproject.repository.api.RecordId;

public interface RecordIdFactory {
    /**
     * Returns the DataInput split into two DataInputs: first one is the master id,
     * second one are the variant properties. The second one should be null if there
     * are no variant properties.
     */
    DataInput[] splitInMasterAndVariant(DataInput dataInput);
    
    RecordId fromBytes(DataInput dataInput, IdGeneratorImpl idGenerator);
    
    RecordId fromString(String string, IdGeneratorImpl idGenerator);
}
