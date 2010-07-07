package org.lilycms.repository.avro;

import java.util.List;

public class RestoredException extends Exception {
    private String originalClass;
    private List<RestoredStackTraceElement> stackTrace;

    public RestoredException(String message, String originalClass, List<RestoredStackTraceElement> stackTrace) {
        super("[remote exception of type " + originalClass + "]" + message);
        this.originalClass = originalClass;
        this.stackTrace = stackTrace;
    }
}
