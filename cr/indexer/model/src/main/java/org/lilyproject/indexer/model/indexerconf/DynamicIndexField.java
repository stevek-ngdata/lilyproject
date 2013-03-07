/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.indexer.model.indexerconf;

import java.util.Set;

import org.lilyproject.repository.api.FieldType;
import org.lilyproject.repository.api.Scope;
import org.lilyproject.util.Pair;

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
