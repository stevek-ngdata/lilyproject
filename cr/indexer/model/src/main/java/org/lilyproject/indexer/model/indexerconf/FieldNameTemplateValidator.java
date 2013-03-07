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

public class FieldNameTemplateValidator extends AbstractNameTemplateValidator {

    private static final Set<Class> SUPPORTED_TYPES = Sets.<Class>newHashSet(
            FieldTemplatePart.class,
            VariantPropertyTemplatePart.class,
            LiteralTemplatePart.class);

    public FieldNameTemplateValidator(Set<String> variantProperties, Set<QName> fields) {
        super(SUPPORTED_TYPES, Collections.<String>emptySet(), Collections.<String>emptySet(), variantProperties,
                fields);
    }
}
