package org.lilyproject.repository.impl.recordid;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.repository.api.RecordId;

import java.util.UUID;

public class UUIDRecordIdFactory implements RecordIdFactory {
    private static final int UUID_LENGTH = 16;
    
    @Override
    public DataInput[] splitInMasterAndVariant(DataInput dataInput) {
        if (dataInput.getSize() - dataInput.getPosition() > UUID_LENGTH) {
            
            DataInput keyInput = new DataInputImpl(((DataInputImpl)dataInput), dataInput.getPosition(),
                    dataInput.getPosition() + 16);
            
            DataInput variantInput = new DataInputImpl(((DataInputImpl)dataInput), dataInput.getPosition() + 16,
                    dataInput.getSize());
            
            return new DataInput[] { keyInput, variantInput };            
        } else {
            return new DataInput[] { dataInput, null };
        }
    }

    @Override
    public RecordId fromBytes(DataInput dataInput, IdGeneratorImpl idGenerator) {
        UUID uuid = new UUID(dataInput.readLong(), dataInput.readLong());
        return new UUIDRecordId(uuid, idGenerator);
    }

    @Override
    public RecordId fromString(String string, IdGeneratorImpl idGenerator) {
        return new UUIDRecordId(string, idGenerator);
    }
}
