package org.lilycms.indexer.conf;

import org.w3c.dom.Element;

public class IndexFieldType {
    private String name;
    private String className;
    /** The SOLR-syntaxed field type XML definition. */
    private Element definition;

    public IndexFieldType(String name, String className, Element definition) {
        this.name = name;
        this.className = className;
        this.definition = definition;
    }

    public String getName() {
        return name;
    }

    public String getClassName() {
        return className;
    }

    public Element getDefinition() {
        return definition;
    }
}
