package org.lilycms.util;

public class ArgumentValidator {
    public static void notNull(Object object, String argName) {
        if (object == null)
            throw new IllegalArgumentException("Null argument: " + argName);
    }
}
