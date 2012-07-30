package org.lilyproject.indexer.model.indexerconf;

public class ConditionalTemplatePart implements TemplatePart {
    private String conditional;
    private String trueString;
    private String falseString;

    public ConditionalTemplatePart(String conditional, String trueString, String falseString) {
        this.conditional = conditional;
        this.trueString = trueString;
        this.falseString = falseString;
    }

    public String getConditional() {
        return conditional;
    }

    public String getTrueString() {
        return trueString;
    }

    public String getFalseString() {
        return falseString;
    }



}
