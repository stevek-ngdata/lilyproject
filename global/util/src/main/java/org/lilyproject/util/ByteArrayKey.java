package org.lilyproject.util;

import java.util.Arrays;

public class ByteArrayKey {
    private byte[] key;
    private int hash;

    public ByteArrayKey(byte[] key) {
        this.key = Arrays.copyOf(key, key.length);
        this.hash = Arrays.hashCode(key);
    }

    public byte[] getKey() {
        return key;
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ByteArrayKey other = (ByteArrayKey) obj;
        return Arrays.equals(key, other.key);
    }
}
