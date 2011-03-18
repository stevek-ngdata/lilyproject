package org.lilyproject.repository.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.lilyproject.bytes.api.DataInput;
import org.lilyproject.bytes.impl.DataInputImpl;
import org.lilyproject.bytes.impl.DataOutputImpl;
import org.lilyproject.util.ArgumentValidator;

public class BlobStoreAccessConfig {

    private String defaultAccess = null;
    private Map<String, Long> limits = new HashMap<String, Long>();

    public BlobStoreAccessConfig(byte[] encodedConfig) {
        DataInput dataInput = new DataInputImpl(encodedConfig);
        this.defaultAccess = dataInput.readUTF();
        int nrOfLimits = dataInput.readInt();
        for (int i = 0; i < nrOfLimits; i++) {
            String name = dataInput.readUTF();
            Long limit = dataInput.readLong();
            limits.put(name, limit);
        }
    }
    
    public byte[] toBytes() {
        DataOutputImpl dataOutput = new DataOutputImpl();
        dataOutput.writeUTF(defaultAccess);
        dataOutput.writeLong(limits.size());
        for (Entry<String, Long> limit : limits.entrySet()) {
            dataOutput.writeUTF(limit.getKey());
            dataOutput.writeLong(limit.getValue());
        }
        return dataOutput.toByteArray();
    }

    public BlobStoreAccessConfig(String defaultAccess) {
        ArgumentValidator.notNull(defaultAccess, "defaultAccess");
        this.defaultAccess = defaultAccess;
    }
    
    public void setDefault(String defaultAccess) {
        ArgumentValidator.notNull(defaultAccess, "defaultAccess");
        this.defaultAccess = defaultAccess;
    }
    
    public String getDefault() {
        return this.defaultAccess;
    }
    
    public void setLimit(String access, long limit) {
        limits.put(access, limit);
    }
    
    public Map<String, Long> getLimits() {
        return limits;
    }
    
}
