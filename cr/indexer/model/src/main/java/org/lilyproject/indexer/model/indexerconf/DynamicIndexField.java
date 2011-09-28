package org.lilyproject.indexer.model.indexerconf;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.util.Pair;

import java.util.Set;

public class DynamicIndexField {
    private WildcardPattern namespace;
    private WildcardPattern name;
    private Set<String> primitiveTypes;
    private Boolean multiValue;
    private Boolean hierarchical;
    private Set<Scope> scopes;
    private boolean continue_;

    private NameTemplate nameTemplate;

    private boolean extractContext;
    private String formatter;

    public DynamicIndexField(WildcardPattern namespace, WildcardPattern name, Set<String> primitiveTypes,
            Boolean multiValue, Boolean hierarchical, Set<Scope> scopes, NameTemplate nameTemplate,
            boolean extractContext, boolean continue_, String formatter) {
        this.namespace = namespace;
        this.name = name;
        this.primitiveTypes = primitiveTypes;
        this.multiValue = multiValue;
        this.hierarchical = hierarchical;
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

        if (primitiveTypes != null) {
            if (!primitiveTypes.contains(fieldType.getValueType().getDeepestValueType().getBaseName())) {
                match.match = false;
                return match;
            }
        }

        if (multiValue != null) {
            if (fieldType.getValueType().isMultiValue() != multiValue) {
                match.match = false;
                return match;
            }
        }

        if (hierarchical != null) {
            if (fieldType.getValueType().isHierarchical() != hierarchical) {
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
