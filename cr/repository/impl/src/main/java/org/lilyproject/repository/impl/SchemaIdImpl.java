package org.lilyproject.repository.impl;

import java.util.Arrays;
import java.util.UUID;

import org.apache.hadoop.hbase.util.Bytes;
import org.lilyproject.repository.api.SchemaId;

public class SchemaIdImpl implements SchemaId {
    
    private UUID uuid;
    private byte[] bytes;
    private String string;

    public SchemaIdImpl(UUID uuid) {
        this.uuid = uuid;
        this.bytes = idToBytes(uuid);
    }
    
    public SchemaIdImpl(byte[] id) {
        this.bytes = id;
    }
    
    public SchemaIdImpl(String id) {
        this.string = id;
        this.uuid = UUID.fromString(id);
        this.bytes = idToBytes(uuid);
    }
    
    public byte[] getBytes() {
        return bytes;
    }
    
    public String toString() {
        if (string == null) {
            if (uuid == null)
                this.uuid = new UUID(Bytes.toLong(bytes, 0, 8), Bytes.toLong(bytes, 8, 8));
            this.string = uuid.toString();
        }
        return string;
    }
    
    private byte[] idToBytes(UUID uuid) {
        byte[] rowId;
        rowId = new byte[16];
        Bytes.putLong(rowId, 0, uuid.getMostSignificantBits());
        Bytes.putLong(rowId, 8, uuid.getLeastSignificantBits());
        return rowId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(bytes);
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
        SchemaIdImpl other = (SchemaIdImpl) obj;
        if (!Arrays.equals(bytes, other.bytes))
            return false;
        return true;
    }
    
    
}
