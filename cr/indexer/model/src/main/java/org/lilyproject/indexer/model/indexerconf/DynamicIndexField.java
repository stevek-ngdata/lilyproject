package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.util.Pair;

import java.util.Set;

public class DynamicIndexField {
    private WildcardPattern namespace;
    private WildcardPattern name;
    private TypePattern typePattern;
    private Set<Scope> scopes;
    private boolean continue_;

    private NameTemplate nameTemplate;

    private boolean extractContext;
    private String formatter;

    public DynamicIndexField(WildcardPattern namespace, WildcardPattern name, TypePattern typePattern,
            Set<Scope> scopes, NameTemplate nameTemplate, boolean extractContext, boolean continue_, String formatter) {
        this.namespace = namespace;
        this.name = name;
        this.typePattern = typePattern;
        this.scopes = scopes;
        this.nameTemplate = nameTemplate;
        this.extractContext = extractContext;
        this.continue_ = continue_;
        this.formatter = formatter;
    }

    public DynamicIndexFieldMatch matches(FieldType fieldType) {
        DynamicIndexFieldMatch match = new DynamicIndexFieldMatch();

        if (namespace != null) {
            Pair<Boolean, String> result = namespace.match(fieldType.getName().getNamespace());
            if (result.getV1()) {
                match.namespaceMatch = result.getV2();
            } else {
                match.match = false;
                return match;
            }
        }

        if (name != null) {
            Pair<Boolean, String> result = name.match(fieldType.getName().getName());
            if (result.getV1()) {
                match.nameMatch = result.getV2();
            } else {
                match.match = false;
                return match;
            }
        }

        if (typePattern != null) {
            if (!typePattern.matches(fieldType.getValueType().getName())) {
                match.match = false;
                return match;
            }
        }

        if (scopes != null) {
            if (!scopes.contains(fieldType.getScope())) {
                match.match = false;
                return match;
            }
        }

        // We've passed all tests and are still here: we have a match
        match.match = true;

        return match;
    }

    public NameTemplate getNameTemplate() {
        return nameTemplate;
    }

    public boolean extractContext() {
        return extractContext;
    }

    public String getFormatter() {
        return formatter;
    }

    public boolean getContinue() {
        return continue_;
    }

    public static class DynamicIndexFieldMatch {
        public String nameMatch;
        public String namespaceMatch;
        public boolean match;
    }
}
