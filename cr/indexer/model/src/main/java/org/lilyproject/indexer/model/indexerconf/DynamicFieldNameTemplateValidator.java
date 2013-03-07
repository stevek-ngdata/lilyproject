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

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Sets;
import org.lilyproject.repository.api.QName;

public class DynamicFieldNameTemplateValidator extends AbstractNameTemplateValidator {

    private static final Set<Class> SUPPORTED_TYPES = Sets.<Class>newHashSet(
            LiteralTemplatePart.class,
            VariableTemplatePart.class,
            ConditionalTemplatePart.class);

    private static final Set<String> SUPPORTED_BOOLEAN_VARIABLES = Sets.newHashSet("list", "multiValue");

    public DynamicFieldNameTemplateValidator(Set<String> variables) {
        super(SUPPORTED_TYPES, variables, SUPPORTED_BOOLEAN_VARIABLES, Collections.<String>emptySet(),
                Collections.<QName>emptySet());
    }

    public DynamicFieldNameTemplateValidator(Set<String> variables, Set<String> booleanVariables) {
        super(SUPPORTED_TYPES, variables, booleanVariables, Collections.<String>emptySet(),
                Collections.<QName>emptySet());
    }
}
