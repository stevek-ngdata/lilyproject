package org.lilyproject.indexer.model.indexerconf;

import java.util.HashSet;
import java.util.Set;

public class DefaultNameTemplateValidator implements NameTemplateValidator {

    private Set<String> variables = new HashSet<String>();
    private Set<String> booleanVariables = new HashSet<String>();

    public DefaultNameTemplateValidator(Set<String> variables, Set<String> booleanVariables) {
        this.variables = variables;
        this.booleanVariables = booleanVariables;
    }

    @Override
    public void validateBooleanVariable(String var) throws NameTemplateException {
        if (!booleanVariables.contains(var)) {
            throw new NameTemplateException("Invalid boolean variable: " + var);
        }
    }

    @Override
    public void validateVariable(String var) throws NameTemplateException {
        if (variables != null && !variables.contains(var)) {
            throw new NameTemplateException("Invalid variable: " + var);
        }
    }

    @Override
    public void validateCondition(String condition, String trueValue, String falseValue) throws NameTemplateException {
        validateBooleanVariable(condition);
    }

}
