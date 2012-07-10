package org.lilyproject.indexer.model.indexerconf;

public class VariableTemplatePart implements TemplatePart {
    private String variablee;

    public VariableTemplatePart(String variable) {
        this.variablee = variable;
    }

    public String getVariable() {
        return variablee;
    }
}

