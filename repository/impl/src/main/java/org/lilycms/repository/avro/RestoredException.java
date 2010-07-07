package org.lilycms.repository.avro;

import java.util.List;

public class RestoredException extends Exception {
    private String originalClass;
    private List<StackTraceElement> stackTrace;

    public RestoredException(String message, String originalClass, List<StackTraceElement> stackTrace) {
        super("[remote exception of type " + originalClass + "]" + message);
        this.originalClass = originalClass;
        this.stackTrace = stackTrace;
    }

    public String getOriginalClass() {
        return originalClass;
    }

    public List<StackTraceElement> getOriginalStackTrace() {
        return stackTrace;
    }
}
