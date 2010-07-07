package org.lilycms.repository.avro;

public class RestoredStackTraceElement {
    private String className;
    private String fileName;
    private int lineNumber;
    private String methodName;
    private boolean nativeMethod;

    public RestoredStackTraceElement(String className, String fileName, int lineNumber, String methodName, boolean nativeMethod) {
        this.className = className;
        this.fileName = fileName;
        this.lineNumber = lineNumber;
        this.methodName = methodName;
        this.nativeMethod = nativeMethod;
    }

    public String getClassName() {
        return className;
    }

    public String getFileName() {
        return fileName;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    public String getMethodName() {
        return methodName;
    }

    public boolean isNativeMethod() {
        return nativeMethod;
    }
}
