package org.lilyproject.indexer.model.indexerconf;

public class NameTemplateEvaluationException extends RuntimeException {
    public NameTemplateEvaluationException(String message, String template) {
        super("Error evaluating " + template + " : " + message);
    }
}
