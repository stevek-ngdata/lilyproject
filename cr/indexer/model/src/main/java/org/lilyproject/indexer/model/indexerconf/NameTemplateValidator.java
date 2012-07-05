package org.lilyproject.indexer.model.indexerconf;

public interface NameTemplateValidator {

    void validateBooleanVariable(String condition) throws NameTemplateException;

    void validateVariable(String expr) throws NameTemplateException;

    void validateCondition(String condition, String trueValue, String falseValue) throws NameTemplateException;

}
