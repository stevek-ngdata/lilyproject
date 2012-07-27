package org.lilyproject.indexer.model.indexerconf;

public interface NameTemplateValidator {

    void validate(NameTemplate nameTemplate) throws NameTemplateException;

}
