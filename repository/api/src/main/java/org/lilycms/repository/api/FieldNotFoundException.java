package org.lilycms.repository.api;

public class FieldNotFoundException extends Exception {

    private final String fieldName;

    public FieldNotFoundException(String fieldName) {
        this.fieldName = fieldName;
    }
    
    @Override
    public String getMessage() {
        return "Field <" + fieldName + "> could not be found.";
    }
}
