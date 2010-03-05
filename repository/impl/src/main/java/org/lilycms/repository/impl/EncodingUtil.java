package org.lilycms.repository.impl;

import java.util.Arrays;

public class EncodingUtil {
    
    public static final byte EXISTS_FLAG = (byte) 0;
    public static final byte DELETE_FLAG = (byte) 1;

    public static boolean isDeletedField(byte[] value) {
        return value[0] == DELETE_FLAG;
    }

    public static byte[] prefixValue(byte[] fieldValue, byte prefix) {
        byte[] prefixedValue;
        prefixedValue = new byte[fieldValue.length + 1];
        prefixedValue[0] = prefix;
        System.arraycopy(fieldValue, 0, prefixedValue, 1, fieldValue.length);
        return prefixedValue;
    }

    public static byte[] stripPrefix(byte[] prefixedValue) {
        return Arrays.copyOfRange(prefixedValue, 1, prefixedValue.length);
    }
}
