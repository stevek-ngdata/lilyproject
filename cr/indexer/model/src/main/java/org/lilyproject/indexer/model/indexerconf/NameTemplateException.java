package org.lilyproject.indexer.model.indexerconf;

public class NameTemplateException extends Exception {
    public NameTemplateException(String message, String template) {
        super("Error with '" + template + "' : " + message);
    }
}
