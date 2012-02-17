package org.lilyproject.repository.impl.recordid;

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
