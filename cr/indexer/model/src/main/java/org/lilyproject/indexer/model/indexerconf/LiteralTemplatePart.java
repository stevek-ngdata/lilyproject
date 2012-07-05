package org.lilyproject.indexer.model.indexerconf;

public class LiteralTemplatePart implements TemplatePart {
    private String string;

    public LiteralTemplatePart(String string) {
        this.string = string;
    }

    public String getString() {
        return string;
    }
}

