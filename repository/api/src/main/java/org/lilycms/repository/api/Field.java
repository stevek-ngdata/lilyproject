package org.lilycms.repository.api;

import java.util.Arrays;

public class Field {

    private String name;
    private byte[] value;
    private boolean versionable; // TODO move this to the FieldType

    public Field(String name, byte[] value, boolean versionable) {
        this.name = name;
        this.value = value;
        this.versionable = versionable;
    }

    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }

    public byte[] getValue() {
        return value;
    }
    
    public void setValue(byte[] value) {
        this.value = value;
    }

    public boolean isVersionable() {
        return versionable;
    }
    
    public void setVersionable(boolean versionable) {
        this.versionable = versionable;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + Arrays.hashCode(value);
        result = prime * result + (versionable ? 1231 : 1237);
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
        Field other = (Field) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (!Arrays.equals(value, other.value))
            return false;
        if (versionable != other.versionable)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "["+name+","+new String(value)+","+versionable+"]";
    }
}

